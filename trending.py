#!/usr/bin/env python3
"""
Fixed trending data fetcher that handles pytrends API changes
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
        # Use an in-memory dictionary for the cache to avoid creating files
        self._cache = {}
        self.db_connection = None
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # No files to close with an in-memory cache
        pass
    
    def connect_database(self):
        """Connect to Supabase Postgres via DATABASE_URL"""
        try:
            import psycopg2
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")
            self.db_connection = psycopg2.connect(database_url)
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
    
    async def fetch_all_trending_data(self) -> List[str]:
        """Fetch trending data using trendspy as primary and fallbacks"""
        keywords = []
        
        if TRENDSPY_AVAILABLE:
            keywords.extend(await self._fetch_trendspy_data())
        
        
        # Remove duplicates and limit
        unique_keywords = list(dict.fromkeys(keywords))
        return unique_keywords[:50]

    def _build_records(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """Build DB records with minimal fields and sources including Wikimedia pageviews."""
        records: List[Dict[str, Any]] = []
        # Add a date bucket for idempotency per day
        run_date = datetime.now(timezone.utc).date().isoformat()
        for kw in keywords:
            try:
                pv = self.get_wikimedia_pageviews(kw)
            except Exception:
                pv = {"pageExists": False, "pageViews30d": 0, "daily": []}
            # Compute metrics
            search_volume = int(pv.get("pageViews30d", 0))
            daily = pv.get("daily", [])
            change_percent = 0.0
            if len(daily) >= 14:
                last7 = sum(daily[-7:])
                prev7 = sum(daily[-14:-7])
                if prev7 > 0:
                    change_percent = ((last7 - prev7) / prev7) * 100.0
                elif last7 > 0:
                    change_percent = 100.0
            trend = 'rising' if change_percent > 10 else ('falling' if change_percent < -10 else 'stable')
            category = self._categorize_keyword(kw)
            difficulty = self._estimate_difficulty(kw)
            cpc = self._estimate_cpc(category)
            sources = {
                "trendspy": True,
                "wikimedia": pv
            }
            record = {
                "keyword": kw,
                "run_date": run_date,
                "search_volume": search_volume,
                "trend": trend,
                "change_percent": round(change_percent, 1),
                "category": category,
                "difficulty": difficulty,
                "cpc": cpc,
                "sources": sources
            }
            records.append(record)
        return records

    def _categorize_keyword(self, keyword: str) -> str:
        k = keyword.lower()
        tech = ['ai', 'artificial intelligence', 'machine learning', 'python', 'javascript', 'software', 'app', 'cloud', 'cyber', 'blockchain']
        sports = [' vs ', 'cup', 'league', 'golf', 'tennis', 'basketball', 'soccer', 'football']
        business = ['stock', 'market', 'economy', 'startup', 'investment', 'crypto']
        entertainment = ['movie', 'film', 'series', 'netflix', 'music', 'concert', 'celebrity']
        health = ['health', 'diet', 'workout', 'fitness', 'med', 'therapy']
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
        return 'General'

    def _estimate_difficulty(self, keyword: str) -> str:
        words = len(keyword.split())
        if words <= 2:
            return 'Low'
        if words <= 4:
            return 'Medium'
        return 'High'

    def _estimate_cpc(self, category: str) -> float:
        if category == 'Business & Finance':
            return 2.5
        if category == 'Technology':
            return 1.8
        if category == 'Health & Wellness':
            return 1.2
        if category == 'Entertainment' or category == 'Sports':
            return 0.8
        return 0.5

    def store_keywords_in_database(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Upsert records into public.trendingkeyword on Supabase."""
        if not self.db_connection:
            return {"success": False, "error": "No database connection"}
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'public' AND table_name = 'trendingkeyword'
                )
            """)
            exists = cursor.fetchone()[0]
            if not exists:
                self.logger.warning("⚠️ Table public.trendingkeyword not found. Upserts will fail.")
            self.logger.info(f"📝 Upserting {len(records)} records into trendingkeyword...")
            processed = 0
            errors = 0
            inserted_keywords = []
            for r in records:
                try:
                    cursor.execute(
                        """
                        INSERT INTO trendingkeyword
                        (keyword, search_volume, trend, change_percent, category, difficulty, cpc, sources, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (keyword)
                        DO UPDATE SET
                          search_volume = EXCLUDED.search_volume,
                          trend = EXCLUDED.trend,
                          change_percent = EXCLUDED.change_percent,
                          category = EXCLUDED.category,
                          difficulty = EXCLUDED.difficulty,
                          cpc = EXCLUDED.cpc,
                          sources = EXCLUDED.sources,
                          updated_at = NOW()
                        """,
                        (
                            r["keyword"], r["search_volume"], r["trend"], r["change_percent"],
                            r["category"], r["difficulty"], r["cpc"], json.dumps(r["sources"])
                        )
                    )
                    processed += 1
                    inserted_keywords.append(r["keyword"])
                    # Log each upsert with compact metrics
                    if processed <= 5 or processed % 25 == 0 or processed == len(records):
                        self.logger.info(
                            f"✅ upserted '{r['keyword']}' vol={r['search_volume']} trend={r['trend']} change={r['change_percent']}%"
                        )
                except Exception as e:
                    errors += 1
                    self.logger.warning(f"⚠️ Failed to upsert '{r['keyword']}': {e}")
            try:
                self.db_connection.commit()
                self.logger.info("🧾 Transaction committed")
            except Exception as e:
                self.logger.error(f"❌ Commit failed: {e}")
                raise
            # Verify sampled keywords exist
            try:
                if inserted_keywords:
                    sample = inserted_keywords[:min(20, len(inserted_keywords))]
                    cursor = self.db_connection.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM trendingkeyword WHERE keyword = ANY(%s)",
                        (sample,)
                    )
                    verified = cursor.fetchone()[0]
                    cursor.close()
                    self.logger.info(f"🔎 Verification: {verified}/{len(sample)} sample keywords present after upsert")
            except Exception as e:
                self.logger.warning(f"⚠️ Verification query failed: {e}")
            cursor.close()
            self.logger.info(f"📦 DB write complete: processed={processed}, errors={errors}")
            return {"success": True, "processed_count": processed, "error_count": errors}
        except Exception as e:
            try:
                self.db_connection.rollback()
                self.logger.info("↩️ Transaction rolled back")
            except Exception:
                pass
            return {"success": False, "error": str(e)}
    
    async def _fetch_trendspy_data(self) -> List[str]:
        """Fetch data from trendspy (trending_now) across regions"""
        keywords = []
        
        try:
            if self.trendspy is None:
                self.logger.info("🔄 Initializing trendspy...")
                self.trendspy = Trends()
                self.logger.info("✅ trendspy initialized")

            regions_env = (os.getenv('TRENDS_GEOS') or 'US,GB,CA,AU').split(',')
            regions = [r.strip().upper() for r in regions_env if r.strip()]

            max_total = int(os.getenv('MAX_KEYWORDS', '50'))

            for region in regions:
                try:
                    await asyncio.sleep(0.2)
                    trends = self.trendspy.trending_now(geo=region)
                    for t in trends:
                        if hasattr(t, 'keyword') and t.keyword:
                            keywords.append(str(t.keyword))
                        if hasattr(t, 'trend_keywords') and t.trend_keywords:
                            for k in list(t.trend_keywords)[:3]:
                                if k:
                                    keywords.append(str(k))
                    self.logger.info(f"✅ trendspy: collected {len(keywords)} so far from {region}")
                    if len(keywords) >= max_total:
                        break
                except Exception as e:
                    self.logger.warning(f"⚠️ trendspy error for {region}: {e}")
                    continue
                
        except Exception as e:
            self.logger.warning(f"⚠️ trendspy methods failed: {e}")
        
        return keywords

    def get_wikimedia_pageviews(self, keyword: str) -> Dict[str, Any]:
        """Lookup a Wikipedia title (multi-language) and return last-30-day pageviews."""
        # Check in-memory cache first
        if keyword in self._cache:
            return self._cache[keyword]

        try:
            candidate_langs = (os.getenv('WIKI_LANGS') or 'en,es,pt,fr,de').split(',')
            candidate_langs = [l.strip() for l in candidate_langs if l.strip()]
            found_title = None
            found_lang = None
            # Try multiple languages to improve hit rate (many trend terms are non-English)
            for lang in candidate_langs:
                try:
                    resp = requests.get(
                        f"https://{lang}.wikipedia.org/w/rest.php/v1/search/title",
                        params={"q": keyword, "limit": 1}, timeout=8
                    )
                    if not resp.ok:
                        continue
                    data = resp.json()
                    pages = data.get("pages") or []
                    if pages:
                        found_title = pages[0].get("title")
                        found_lang = lang
                        break
                except Exception:
                    continue
            if not found_title:
                # As a last resort, try using the raw keyword against English
                found_title = keyword
                found_lang = 'en'
            
            end = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d') + '00'
            start = (datetime.now(timezone.utc) - timedelta(days=31)).strftime('%Y%m%d') + '00'
            url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
                f"{found_lang}.wikipedia.org/all-access/user/{quote(found_title, safe='')}/daily/{start}/{end}"
            )
            pv = requests.get(url, timeout=10)
            views = 0
            daily = []
            if pv.ok:
                items = (pv.json() or {}).get("items") or []
                views = sum(int(i.get("views", 0)) for i in items)
                daily = [int(i.get("views", 0)) for i in items][-30:]
            else:
                self.logger.info(f"ℹ️ No pageviews for '{found_title}' ({found_lang}), status={pv.status_code}")
            if views == 0:
                self.logger.debug(f"🔎 Zero pageviews for '{keyword}' → '{found_title}' ({found_lang})")
            result = {
                "pageExists": bool(found_title),
                "title": found_title,
                "lang": found_lang,
                "pageViews30d": views,
                "daily": daily
            }
            # Store the result in the in-memory cache
            self._cache[keyword] = result
            return result
        except Exception as e:
            self.logger.warning(f"⚠️ Wikimedia lookup failed for '{keyword}': {e}")
            result = {"pageExists": False, "pageViews30d": 0, "daily": []}
            self._cache[keyword] = result
            return result
    
    # Fallback keyword logic removed — only real data (trendspy + Wikimedia) is used
    
    async def run(self) -> Dict[str, Any]:
        """Main run method"""
        try:
            start_time = time.time()
            keywords = await self.fetch_all_trending_data()
            end_time = time.time()
            
            pushed = None
            if os.getenv('DATABASE_URL'):
                self.logger.info("🔌 DATABASE_URL detected; attempting DB connection...")
                if self.connect_database():
                    self.logger.info("🧩 Building records for DB upsert...")
                    records = self._build_records(keywords)
                    self.logger.info(f"🧮 Built {len(records)} records. Beginning upsert...")
                    pushed = self.store_keywords_in_database(records)
                else:
                    self.logger.error("❌ Skipping DB write due to failed connection")
            else:
                self.logger.info("ℹ️ DATABASE_URL not set; skipping DB write")
            
            result = {
                'success': True,
                'keywords_count': len(keywords),
                'keywords_sample': keywords[:10],
                'execution_time': round(end_time - start_time, 2),
                'timestamp': datetime.now().isoformat()
            }
            if pushed is not None:
                result['db'] = pushed
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Job execution failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Module-level entrypoint for Vercel function wrapper and CLI
async def main() -> Dict[str, Any]:
    async with TrendingDataFetcher() as fetcher:
        return await fetcher.run()  

if __name__ == "__main__":
    import asyncio as _asyncio
    import json as _json
    try:
        _result = _asyncio.run(main())
        print(_json.dumps(_result, indent=2))
    except KeyboardInterrupt:
        pass