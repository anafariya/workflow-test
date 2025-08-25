# Vercel Deployment Guide for Cron Job

## Project Structure
This is a standalone cron job project that runs independently from the main backend/frontend.

## Prerequisites

1. Install Vercel CLI:
```bash
npm i -g vercel
```

2. Login to Vercel:
```bash
vercel login
```

## Environment Variables

Set these environment variables in your Vercel project:

```bash
# Required
DATABASE_URL=postgresql://username:password@host:port/database

# Optional (with defaults)
TRENDS_GEOS=US,GB,CA,AU
MAX_KEYWORDS=50
WIKI_LANGS=en,es,pt,fr,de
```

## Deployment Steps

1. **Navigate to cron-job directory:**
```bash
cd cron-job
```

2. **Deploy to Vercel:**
```bash
vercel --prod
```

3. **Set Environment Variables:**
```bash
vercel env add DATABASE_URL
vercel env add TRENDS_GEOS
vercel env add MAX_KEYWORDS
vercel env add WIKI_LANGS
```

4. **Redeploy with Environment Variables:**
```bash
vercel --prod
```

## Cron Job Schedule

The cron job runs daily at midnight UTC (`0 0 * * *`).

## Testing

Test the function locally:
```bash
cd cron-job
python3 trending.py
```

Test the Vercel handler:
```bash
cd cron-job/api/cron
python trending.py
```

## Monitoring

Check Vercel dashboard for:
- Function logs
- Execution times
- Error rates
- Cron job status

## Troubleshooting

1. **Database Connection Issues:**
   - Verify `DATABASE_URL` is correct
   - Check Supabase RLS policies
   - Ensure network access

2. **Import Errors:**
   - Verify all dependencies in `requirements.txt`
   - Check Python runtime version (3.11)

3. **Timeout Issues:**
   - Vercel functions have 10s timeout limit
   - Consider optimizing API calls
   - Add more logging for debugging

## Project Files

- `trending.py` - Main cron job logic
- `api/cron/trending.py` - Vercel serverless function handler
- `vercel.json` - Vercel configuration
- `requirements.txt` - Python dependencies
- `test.py` - Test suite
- `env.example` - Environment variables template
