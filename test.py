#!/usr/bin/env python3
"""
Test script for Python Cron Job
Tests database connection and trending data fetching.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv
from trending import TrendingDataFetcher

# Load environment variables
load_dotenv()

async def test_database_connection():
    """Test database connection"""
    print("🔍 Testing database connection...")
    
    async with TrendingDataFetcher() as fetcher:
        if fetcher.connect_database():
            print("✅ Database connection successful")
            return True
        else:
            print("❌ Database connection failed")
            return False

async def test_trending_fetch():
    """Test trending data fetching"""
    print("🔍 Testing trending data fetch...")
    
    async with TrendingDataFetcher() as fetcher:
        try:
            keywords = await fetcher.fetch_all_trending_data()
            print(f"✅ Fetched {len(keywords)} trending keywords")
            print(f"📊 Sample keywords: {keywords[:5]}")
            return True
        except Exception as e:
            print(f"❌ Trending fetch failed: {e}")
            return False

async def test_full_job():
    """Test the complete job"""
    print("🔍 Testing complete job...")
    
    async with TrendingDataFetcher() as fetcher:
        try:
            result = await fetcher.run()
            print(f"✅ Job completed: {result}")
            return result.get('success', False)
        except Exception as e:
            print(f"❌ Job failed: {e}")
            return False

async def main():
    """Main test function"""
    print("🧪 Python Cron Job Test Suite")
    print("=" * 50)
    
    # Check environment
    if not os.getenv('DATABASE_URL'):
        print("❌ DATABASE_URL not set")
        print("Please set DATABASE_URL in your .env file")
        return False
    
    print(f"📋 Environment: {os.getenv('ENVIRONMENT', 'development')}")
    print(f"🗄️ Database: {'Set' if os.getenv('DATABASE_URL') else 'Not set'}")
    
    # Run tests
    tests = [
        ("Database Connection", test_database_connection),
        ("Trending Data Fetch", test_trending_fetch),
        ("Complete Job", test_full_job)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔍 Running {test_name}...")
        try:
            result = await test_func()
            results.append((test_name, result))
            print(f"{'✅' if result else '❌'} {test_name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {test_name}: {status}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The cron job is ready to deploy.")
        return True
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
