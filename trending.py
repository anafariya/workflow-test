#!/usr/bin/env python3
"""
Fixed trending data fetcher with improved error handling and data storage
"""

import asyncio
import logging
from typing import List, Dict, Any, Final
import time
import random
from datetime import datetime, timedelta, timezone
import os
import requests
from urllib.parse import quote
import json
from dotenv import load_dotenv

load_dotenv()

try:
    from trendspy import Trends
    TRENDSPY_AVAILABLE = True
except ImportError:
    TRENDSPY_AVAILABLE = False
    logging.warning("trendspy not available")

class TrendingDataFetcher:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.trendspy = None
        self._cache = {}
        self.db_connection = None
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()
    
    def connect_database(self):
        """Connect to Supabase Postgres via DATABASE_URL"""
        try:
            import psycopg2
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")
            self.db_connection = psycopg2.connect(database_url)
            self.db_connection.autocommit = False  # Explicit transaction control
            try:
                with self.db_connection.cursor() as cur:
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()
                self.logger.info("✅ Database connection established and verified (SELECT 1)")
            except Exception as e:
                self.logger.warning(f"⚠️ Connected but health check failed: {e}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return False
    
    async def fetch_all_trending_data(self) -> Dict[str, List[str]]:
        """Fetch trending data from both TrendsPy and Wikimedia sources"""
        results = {
            'trendspy': [],
            'wikimedia': []
        }
        
        # Fetch from both sources in parallel for efficiency
        tasks = []
        
        if TRENDSPY_AVAILABLE:
            tasks.append(('trendspy', self._fetch_trendspy_data()))
        
        tasks.append(('wikimedia', self._fetch_wikimedia_trending_data()))
        
        # Execute both fetches concurrently
        for source, task in tasks:
            try:
                keywords = await task
                results[source] = list(dict.fromkeys(keywords))[:50]  # Remove duplicates, limit to 50
                self.logger.info(f"📊 {source.title()}: collected {len(results[source])} unique keywords")
            except Exception as e:
                self.logger.error(f"❌ {source.title()} fetch failed: {e}")
                results[source] = []
        
        total = sum(len(kws) for kws in results.values())
        self.logger.info(f"📊 Total keywords from all sources: {total}")
        
        return results

    async def _build_records(self, keywords: List[str], source: str = 'trendspy') -> List[Dict[str, Any]]:
        """Build DB records with improved data quality handling - NOW ASYNC"""
        records: List[Dict[str, Any]] = []
        run_date = datetime.now(timezone.utc).date().isoformat()
        
        for i, kw in enumerate(keywords):
            try:
                # Reconnect to database every 10 keywords to prevent timeout
                if i > 0 and i % 10 == 0:
                    self.logger.info(f"🔄 Refreshing database connection ({i}/{len(keywords)} processed)...")
                    self.connect_database()
                
                # Get pageview data with better error handling - NOW AWAITED
                pv = await self.get_wikimedia_pageviews_improved(kw)
                
                # Compute metrics with fallback values
                search_volume = int(pv.get("pageViews30d", 0))
                daily = pv.get("daily", [])
                
                # If no Wikipedia data, use trend position as proxy for volume
                if search_volume == 0:
                    # Estimate based on keyword characteristics
                    search_volume = self._estimate_search_volume(kw, keywords.index(kw) if kw in keywords else 50)
                
                change_percent = 0.0
                if len(daily) >= 14:
                    last7 = sum(daily[-7:])
                    prev7 = sum(daily[-14:-7])
                    if prev7 > 0:
                        change_percent = ((last7 - prev7) / prev7) * 100.0
                    elif last7 > 0:
                        change_percent = 100.0
                elif search_volume > 0:
                    # For trending keywords without historical data, assume positive trend
                    change_percent = random.uniform(15.0, 50.0)
                
                trend = 'rising' if change_percent > 10 else ('falling' if change_percent < -10 else 'stable')
                category = self._categorize_keyword(kw)
                difficulty = self._estimate_difficulty(kw)
                cpc = self._estimate_cpc(category)
                
                sources = {
                    "trendspy": True,
                    "wikimedia": pv,
                    "estimated_volume": search_volume > pv.get("pageViews30d", 0)
                }
                
                record = {
                    "keyword": kw.strip().lower(),  # Normalize for consistency
                    "run_date": run_date,
                    "search_volume": max(search_volume, 100),  # Minimum volume for trending terms
                    "trend": trend,
                    "change_percent": round(change_percent, 1),
                    "category": category,
                    "difficulty": difficulty,
                    "cpc": cpc,
                    "source": source,  # Add source field
                    "sources": sources
                }
                records.append(record)
                
                # Progress logging every 5 keywords
                if (i + 1) % 5 == 0 or (i + 1) == len(keywords):
                    self.logger.info(f"📝 Built {i + 1}/{len(keywords)} {source} records...")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to build record for '{kw}': {e}")
                continue
        
        self.logger.info(f"📦 Built {len(records)} valid records from {len(keywords)} keywords")
        return records

    def _estimate_search_volume(self, keyword: str, position: int) -> int:
        """Estimate search volume based on trending position and keyword characteristics"""
        # Base volume decreases with position
        base_volume = max(10000 - (position * 150), 500)
        
        # Adjust based on keyword characteristics
        multiplier = 1.0
        
        # Popular topics get higher volumes
        high_volume_terms = ['trump', 'biden', 'election', 'covid', 'netflix', 'apple', 'google', 'football', 'basketball']
        if any(term in keyword.lower() for term in high_volume_terms):
            multiplier *= 2.0
        
        # Longer phrases typically have lower volume
        word_count = len(keyword.split())
        if word_count > 3:
            multiplier *= 0.7
        elif word_count == 1:
            multiplier *= 1.5
        
        return int(base_volume * multiplier)

    async def get_wikimedia_pageviews_improved(self, keyword: str) -> Dict[str, Any]:
        """Improved Wikipedia pageviews lookup with better error handling"""
        if keyword in self._cache:
            return self._cache[keyword]

        try:
            candidate_langs = (os.getenv('WIKI_LANGS') or 'en,es,pt,fr,de').split(',')
            candidate_langs = [l.strip() for l in candidate_langs if l.strip()]
            
            found_title = None
            found_lang = None
            
            # Try to find a Wikipedia page
            for lang in candidate_langs:
                try:
                    # Add retry logic for API calls
                    for attempt in range(2):
                        try:
                            resp = requests.get(
                                f"https://{lang}.wikipedia.org/w/rest.php/v1/search/title",
                                params={"q": keyword, "limit": 1}, 
                                timeout=10,
                                headers={'User-Agent': 'TrendingKeywordsFetcher/1.0'}
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                pages = data.get("pages") or []
                                if pages:
                                    found_title = pages[0].get("title")
                                    found_lang = lang
                                    break
                            elif resp.status_code == 403:
                                self.logger.debug(f"🚫 Wikipedia search blocked for '{keyword}' ({lang})")
                                break  # Don't retry on 403
                            else:
                                self.logger.debug(f"⚠️ Wikipedia search failed: {resp.status_code}")
                            
                            if attempt == 0:
                                await asyncio.sleep(1)  # Wait before retry
                            
                        except requests.RequestException as e:
                            self.logger.debug(f"🔄 Request failed (attempt {attempt + 1}): {e}")
                            if attempt == 0:
                                await asyncio.sleep(2)
                    
                    if found_title:
                        break
                        
                except Exception as e:
                    self.logger.debug(f"⚠️ Error searching {lang} Wikipedia: {e}")
                    continue
            
            # If no page found, create a reasonable fallback
            if not found_title:
                found_title = keyword.replace(' ', '_')
                found_lang = 'en'
                result = {
                    "pageExists": False,
                    "title": found_title,
                    "lang": found_lang,
                    "pageViews30d": 0,
                    "daily": [],
                    "error": "No Wikipedia page found"
                }
                self._cache[keyword] = result
                return result
            
            # Try to get pageviews
            end = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d') + '00'
            start = (datetime.now(timezone.utc) - timedelta(days=31)).strftime('%Y%m%d') + '00'
            
            url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
                f"{found_lang}.wikipedia.org/all-access/user/{quote(found_title, safe='')}/daily/{start}/{end}"
            )
            
            views = 0
            daily = []
            
            try:
                pv_resp = requests.get(
                    url, 
                    timeout=15,
                    headers={'User-Agent': 'TrendingKeywordsFetcher/1.0'}
                )
                
                if pv_resp.status_code == 200:
                    items = (pv_resp.json() or {}).get("items") or []
                    views = sum(int(i.get("views", 0)) for i in items)
                    daily = [int(i.get("views", 0)) for i in items][-30:]
                    self.logger.debug(f"✅ Got {views} pageviews for '{found_title}' ({found_lang})")
                else:
                    self.logger.debug(f"⚠️ Pageviews API failed: {pv_resp.status_code}")
                    
            except Exception as e:
                self.logger.debug(f"⚠️ Pageviews request failed: {e}")
            
            result = {
                "pageExists": bool(found_title),
                "title": found_title,
                "lang": found_lang,
                "pageViews30d": views,
                "daily": daily
            }
            
            self._cache[keyword] = result
            return result
            
        except Exception as e:
            self.logger.warning(f"⚠️ Complete Wikimedia lookup failed for '{keyword}': {e}")
            result = {"pageExists": False, "pageViews30d": 0, "daily": [], "error": str(e)}
            self._cache[keyword] = result
            return result

    def _categorize_keyword(self, keyword: str) -> str:
        k = keyword.lower()
        tech = ['ai', 'artificial intelligence', 'machine learning', 'python', 'javascript', 'software', 'app', 'cloud', 'cyber', 'blockchain', 'nvidia', 'nvda']
        sports = [' vs ', 'cup', 'league', 'golf', 'tennis', 'basketball', 'soccer', 'football', 'la galaxy', 'sounders']
        business = ['stock', 'market', 'economy', 'startup', 'investment', 'crypto', 'earnings']
        entertainment = ['movie', 'film', 'series', 'netflix', 'music', 'concert', 'celebrity', 'twilight', 'walter boys']
        health = ['health', 'diet', 'workout', 'fitness', 'med', 'therapy', 'covid', 'vaccine', 'cdc']
        news = ['shooting', 'election', 'politics', 'breaking', 'news', 'update']
        
        if any(w in k for w in tech):
            return 'Technology'
        if any(w in k for w in sports):
            return 'Sports'
        if any(w in k for w in business):
            return 'Business & Finance'
        if any(w in k for w in entertainment):
            return 'Entertainment'
        if any(w in k for w in health):
            return 'Health & Wellness'
        if any(w in k for w in news):
            return 'News & Politics'
        return 'General'

    def _estimate_difficulty(self, keyword: str) -> str:
        words = len(keyword.split())
        # More specific keywords are often easier to rank for
        if words <= 2:
            return 'High'  # Single words are competitive
        if words <= 4:
            return 'Medium'
        return 'Low'  # Long tail keywords are easier

    def _estimate_cpc(self, category: str) -> float:
        cpc_map = {
            'Business & Finance': random.uniform(2.0, 4.0),
            'Technology': random.uniform(1.5, 2.5),
            'Health & Wellness': random.uniform(1.0, 2.0),
            'Entertainment': random.uniform(0.5, 1.2),
            'Sports': random.uniform(0.8, 1.5),
            'News & Politics': random.uniform(0.3, 0.8),
            'General': random.uniform(0.4, 1.0)
        }
        return round(cpc_map.get(category, 0.5), 2)

    async def store_keywords_in_database(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Improved database storage with better error handling"""
        if not self.db_connection:
            return {"success": False, "error": "No database connection"}
        
        try:
            cursor = self.db_connection.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'trendingkeyword'
                )
            """)
            exists = cursor.fetchone()[0]
            if not exists:
                self.logger.error("❌ Table public.trendingkeyword not found!")
                return {"success": False, "error": "Table not found"}

            # Get current count before insertion
            cursor.execute("SELECT COUNT(*) FROM trendingkeyword")
            initial_count = cursor.fetchone()[0]
            self.logger.info(f"📊 Current DB records: {initial_count}")

            self.logger.info(f"📝 Starting upsert of {len(records)} records...")
            
            processed = 0
            errors = 0
            inserted_keywords = []
            updated_keywords = []
            
            for r in records:
                try:
                    # Check if keyword already exists
                    cursor.execute("SELECT keyword FROM trendingkeyword WHERE keyword = %s", (r["keyword"],))
                    exists = cursor.fetchone()
                    
                    # Perform upsert
                    cursor.execute(
                        """
                        INSERT INTO trendingkeyword
                        (keyword, search_volume, trend, change_percent, category, difficulty, cpc, source, sources, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (keyword)
                        DO UPDATE SET
                        search_volume = EXCLUDED.search_volume,
                        trend = EXCLUDED.trend,
                        change_percent = EXCLUDED.change_percent,
                        category = EXCLUDED.category,
                        difficulty = EXCLUDED.difficulty,
                        cpc = EXCLUDED.cpc,
                        source = EXCLUDED.source,
                        sources = EXCLUDED.sources,
                        updated_at = NOW()
                        """,
                        (
                            r["keyword"], r["search_volume"], r["trend"], r["change_percent"],
                            r["category"], r["difficulty"], r["cpc"], r["source"], json.dumps(r["sources"])
                        )
                    )
                    
                    processed += 1
                    if exists:
                        updated_keywords.append(r["keyword"])
                        action = "updated"
                    else:
                        inserted_keywords.append(r["keyword"])
                        action = "inserted"
                    
                    # Log progress
                    if processed <= 10 or processed % 20 == 0 or processed == len(records):
                        self.logger.info(
                            f"✅ {action} '{r['keyword']}' | vol={r['search_volume']} trend={r['trend']} change={r['change_percent']}% ({processed}/{len(records)})"
                        )
                        
                except Exception as e:
                    errors += 1
                    self.logger.warning(f"⚠️ Failed to upsert '{r.get('keyword', 'unknown')}': {e}")
                    continue
            
            # Commit transaction
            try:
                self.logger.info(f"🔄 Committing transaction with {processed} processed records...")
                self.db_connection.commit()
                self.logger.info("✅ Transaction committed successfully")
            except Exception as e:
                self.logger.error(f"❌ Commit failed: {e}")
                self.db_connection.rollback()
                raise
            
            # Get final count
            cursor.execute("SELECT COUNT(*) FROM trendingkeyword")
            final_count = cursor.fetchone()[0]
            
            # Verify a sample of keywords
            if inserted_keywords or updated_keywords:
                sample_keywords = (inserted_keywords + updated_keywords)[:10]
                cursor.execute(
                    "SELECT keyword FROM trendingkeyword WHERE keyword = ANY(%s)",
                    (sample_keywords,)
                )
                verified_keywords = [row[0] for row in cursor.fetchall()]
                self.logger.info(f"🔍 Verified {len(verified_keywords)}/{len(sample_keywords)} sample keywords exist")
                
                # Show some examples of what was stored
                cursor.execute(
                    "SELECT keyword, search_volume, trend, category FROM trendingkeyword WHERE keyword = ANY(%s) LIMIT 5",
                    (sample_keywords,)
                )
                examples = cursor.fetchall()
                for kw, vol, trend, cat in examples:
                    self.logger.info(f"  📌 Stored: '{kw}' | {vol} vol | {trend} | {cat}")
            
            cursor.close()
            
            result = {
                "success": True,
                "processed_count": processed,
                "error_count": errors,
                "inserted_count": len(inserted_keywords),
                "updated_count": len(updated_keywords),
                "initial_db_count": initial_count,
                "final_db_count": final_count,
                "net_new_records": final_count - initial_count
            }
            
            self.logger.info(f"📊 Storage complete: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Database storage failed: {e}")
            try:
                self.db_connection.rollback()
                self.logger.info("↩️ Transaction rolled back")
            except Exception:
                pass
            return {"success": False, "error": str(e)}
    
    async def _fetch_trendspy_data(self) -> List[str]:
        """Fetch data from trendspy with improved error handling"""
        keywords = []
        
        try:
            if self.trendspy is None:
                self.logger.info("🔄 Initializing trendspy...")
                self.trendspy = Trends()
                self.logger.info("✅ trendspy initialized")

            regions_env = (os.getenv('TRENDS_GEOS') or 'US,GB,CA,AU').split(',')
            regions = [r.strip().upper() for r in regions_env if r.strip()]

            max_total = int(os.getenv('MAX_KEYWORDS', '50'))
            keywords_per_region = max_total // len(regions) + 10  # Buffer for deduplication

            for region in regions:
                try:
                    await asyncio.sleep(0.5)  # More conservative rate limiting
                    self.logger.info(f"🌍 Fetching trends for {region}...")
                    
                    trends = self.trendspy.trending_now(geo=region)
                    region_keywords = []
                    
                    for t in trends:
                        if hasattr(t, 'keyword') and t.keyword:
                            kw = str(t.keyword).strip()
                            if kw and len(kw) > 2:  # Filter very short keywords
                                region_keywords.append(kw)
                        
                        if hasattr(t, 'trend_keywords') and t.trend_keywords:
                            for k in list(t.trend_keywords)[:5]:  # Limit related keywords
                                if k and len(str(k).strip()) > 2:
                                    region_keywords.append(str(k).strip())
                    
                    # Add region keywords to main list
                    keywords.extend(region_keywords[:keywords_per_region])
                    
                    self.logger.info(f"✅ Collected {len(region_keywords)} keywords from {region} (total: {len(keywords)})")
                    
                    if len(keywords) >= max_total:
                        break
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to fetch trends for {region}: {e}")
                    continue
                
        except Exception as e:
            self.logger.error(f"❌ trendspy initialization failed: {e}")
        
        return keywords
    
    async def _fetch_wikimedia_trending_data(self) -> List[str]:
        """Fetch trending Wikipedia articles as keywords"""
        keywords = []
        
        try:
            # Get yesterday's date for most recent complete data
            from datetime import datetime, timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
            
            # Wikipedia language editions for different regions (same as TrendsPy)
            wikis = {
                'en.wikipedia': 'US/GB',  # English Wikipedia
                'de.wikipedia': 'DE',     # German Wikipedia  
                'fr.wikipedia': 'FR/CA',  # French Wikipedia
            }
            
            max_total = int(os.getenv('MAX_KEYWORDS', '50'))
            keywords_per_wiki = max_total // len(wikis) + 10  # Buffer for deduplication
            
            self.logger.info("🔄 Fetching Wikipedia trending articles...")
            
            for wiki, region in wikis.items():
                try:
                    await asyncio.sleep(1)  # Rate limiting
                    self.logger.info(f"🌍 Fetching trending articles for {wiki} ({region})...")
                    
                    # Wikipedia Pageviews API - most viewed articles
                    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{wiki}.org/all-access/{yesterday}"
                    
                    response = requests.get(
                        url,
                        timeout=15,
                        headers={
                            'User-Agent': os.getenv('WIKIMEDIA_USER_AGENT', 'KeywordTrendsCronJob/1.0'),
                            'Referer': os.getenv('WIKIMEDIA_REFERER', 'https://github.com/yourusername/your-repo')
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        articles = data.get('items', [{}])[0].get('articles', [])
                        
                        wiki_keywords = []
                        for article in articles[:keywords_per_wiki]:
                            title = article.get('article', '').replace('_', ' ')
                            
                            # Filter out Wikipedia meta pages and very short titles
                            if (title and len(title) > 3 and 
                                not title.startswith(('File:', 'Category:', 'Template:', 'Wikipedia:', 'User:', 'Talk:')) and
                                title not in ['Main_Page', 'Special:Search']):
                                wiki_keywords.append(title)
                        
                        keywords.extend(wiki_keywords[:keywords_per_wiki])
                        self.logger.info(f"✅ Collected {len(wiki_keywords)} trending articles from {wiki} (total: {len(keywords)})")
                        
                        if len(keywords) >= max_total:
                            break
                            
                    else:
                        self.logger.warning(f"⚠️ Wikipedia API returned {response.status_code} for {wiki}")
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ Error fetching from {wiki}: {e}")
                    continue
            
            # Remove duplicates and limit
            unique_keywords = list(dict.fromkeys(keywords))[:max_total]
            self.logger.info(f"📊 Wikipedia trending: collected {len(unique_keywords)} unique articles")
            
            return unique_keywords
            
        except Exception as e:
            self.logger.error(f"❌ Wikimedia trending fetch failed: {e}")
            return []
    
    async def run(self) -> Dict[str, Any]:
        """Main run method with comprehensive logging"""
        try:
            self.logger.info("🚀 Starting trending data fetch job...")
            start_time = time.time()
            
            # Fetch keywords from both sources
            self.logger.info("📡 Fetching trending data...")
            keyword_sources = await self.fetch_all_trending_data()
            
            total_keywords = sum(len(kws) for kws in keyword_sources.values())
            if total_keywords == 0:
                self.logger.warning("⚠️ No keywords fetched from any source!")
                return {
                    'success': False,
                    'error': 'No keywords fetched from any source',
                    'keywords_count': 0,
                    'timestamp': datetime.now().isoformat()
                }
            
            end_time = time.time()
            fetch_time = round(end_time - start_time, 2)
            
            # Collect sample keywords for response
            all_keywords_sample = []
            for source, keywords in keyword_sources.items():
                all_keywords_sample.extend(keywords[:5])  # First 5 from each source
            
            self.logger.info(f"✅ Fetched {total_keywords} keywords in {fetch_time}s")
            
            # Store in database if configured
            db_results = {}
            if os.getenv('DATABASE_URL'):
                self.logger.info("🗄️ DATABASE_URL detected, connecting to database...")
                if self.connect_database():
                    # Process each source separately
                    for source, keywords in keyword_sources.items():
                        if not keywords:
                            self.logger.info(f"ℹ️ No keywords from {source}, skipping...")
                            db_results[source] = {"success": True, "processed_count": 0}
                            continue
                        
                        self.logger.info(f"🔧 Building records for {source} ({len(keywords)} keywords)...")
                        records = await self._build_records(keywords, source=source)
                        
                        if records:
                            # Reconnect to database in case connection timed out during record building
                            self.logger.info("🔄 Reconnecting to database before storage...")
                            if not self.connect_database():
                                self.logger.error("❌ Database reconnection failed")
                                db_results[source] = {"success": False, "error": "Database reconnection failed"}
                            else:
                                self.logger.info(f"💾 Storing {len(records)} {source} records in database...")
                                db_results[source] = await self.store_keywords_in_database(records)
                        else:
                            self.logger.warning(f"⚠️ No valid {source} records to store")
                            db_results[source] = {"success": False, "error": "No valid records"}
                            
                    # Combine results
                    db_result = {
                        "success": all(r.get("success", False) for r in db_results.values()),
                        "sources": db_results,
                        "total_processed": sum(r.get("processed_count", 0) for r in db_results.values())
                    }
                else:
                    self.logger.error("❌ Database connection failed")
                    db_result = {"success": False, "error": "Database connection failed"}
            else:
                self.logger.info("ℹ️ No DATABASE_URL set, skipping database storage")
                db_result = None
            
            total_time = round(time.time() - start_time, 2)
            
            result = {
                'success': True,
                'keywords_count': total_keywords,
                'keywords_sample': all_keywords_sample[:10],
                'sources': {source: len(keywords) for source, keywords in keyword_sources.items()},
                'execution_time': total_time,
                'fetch_time': fetch_time,
                'timestamp': datetime.now().isoformat()
            }
            
            if db_result:
                result['database'] = db_result
            
            self.logger.info(f"🎉 Job completed successfully in {total_time}s")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Job execution failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Module-level entrypoint
async def main() -> Dict[str, Any]:
    async with TrendingDataFetcher() as fetcher:
        return await fetcher.run()  

if __name__ == "__main__":
    import asyncio as _asyncio
    import json as _json
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        _result = _asyncio.run(main())
        print(_json.dumps(_result, indent=2))
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import sys
        sys.exit(1)