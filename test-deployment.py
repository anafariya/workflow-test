#!/usr/bin/env python3
"""
Test script to verify Vercel deployment works locally
"""

import os
import sys
import asyncio
import json
from dotenv import load_dotenv
load_dotenv()
def test_local_function():
    """Test the trending function locally"""
    try:
        from trending import main as run_trending_job
        
        print("🧪 Testing trending function locally...")
        result = asyncio.run(run_trending_job())
        
        print("✅ Function executed successfully!")
        print(f"📊 Keywords processed: {result.get('keywords_count', 0)}")
        print(f"⏱️ Execution time: {result.get('execution_time', 0)}s")
        
        if 'db' in result:
            db_result = result['db']
            if db_result.get('success'):
                print(f"💾 Database: {db_result.get('processed_count', 0)} records processed")
            else:
                print(f"❌ Database error: {db_result.get('error', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_vercel_handler():
    """Test the Vercel handler function"""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("trending_handler", "api/cron/trending.py")
        trending_handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(trending_handler)
        handler = trending_handler.handler
        
        print("\n🧪 Testing Vercel handler...")
        
        # Mock request and context
        class MockRequest:
            pass
        
        class MockContext:
            pass
        
        request = MockRequest()
        context = MockContext()
        
        response = handler(request, context)
        
        print(f"✅ Handler returned status: {response['statusCode']}")
        
        if response['statusCode'] == 200:
            body = json.loads(response['body'])
            print(f"📊 Response: {body.get('message', 'No message')}")
            return True
        else:
            print(f"❌ Handler failed: {response['body']}")
            return False
            
    except Exception as e:
        print(f"❌ Handler test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Vercel Deployment Test for Cron Job")
    print("=" * 50)
    
    # Check environment
    if not os.getenv('DATABASE_URL'):
        print("⚠️ DATABASE_URL not set - database operations will be skipped")
    
    # Test local function
    local_success = test_local_function()
    
    # Test Vercel handler
    handler_success = test_vercel_handler()
    
    print("\n" + "=" * 50)
    if local_success and handler_success:
        print("🎉 All tests passed! Ready for Vercel deployment.")
        print("\n📋 Next steps:")
        print("1. cd cron-job")
        print("2. vercel --prod")
        print("3. vercel env add DATABASE_URL")
        print("4. vercel --prod")
    else:
        print("❌ Some tests failed. Please fix issues before deploying.")
