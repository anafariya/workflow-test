#!/usr/bin/env python3
"""
Vercel API endpoint for trending keywords cron job
This file is automatically called by Vercel's cron service.
"""

import os
import sys
import asyncio
import json
from datetime import datetime

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from trending import main as run_trending_job

def handler(request, context):
    """Vercel serverless function handler"""
    try:
        # Run the trending job
        result = asyncio.run(run_trending_job())
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Trending keywords job completed',
                'result': result,
                'timestamp': datetime.now().isoformat()
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }

# For local testing
if __name__ == "__main__":
    result = asyncio.run(run_trending_job())
    print(json.dumps(result, indent=2))
