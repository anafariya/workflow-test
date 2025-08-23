# Cron Job Deployment Guide

This guide covers all the ways to deploy the trending keywords cron job.

## Quick Start

### Python Version (Recommended)

1. **Install Python dependencies**:
   ```bash
   cd cron-job
   pip install -r requirements.txt
   ```

2. **Test the setup**:
   ```bash
   python test.py
   ```

3. **Run manually**:
   ```bash
   python trending.py
   ```



## Environment Variables

### Required Variables

```bash
# Database Configuration (Required)
# Use your Supabase PostgreSQL connection string
DATABASE_URL=postgresql://postgres.project-ref:your-password@aws-0-region.pooler.supabase.com:6543/postgres

# Environment (Required)
ENVIRONMENT=development
```

### Optional Variables

```bash
# Logging Configuration
LOG_LEVEL=info

# Data Cleanup Settings
CLEANUP_DAYS=7

# Trending Keywords Settings
TRENDING_KEYWORDS_COUNT=50
TRENDING_SOURCES_COUNT=3

# Python Settings
PYTHON_VERSION=3.11
PYTHONPATH=.

# API Rate Limiting
GOOGLE_TRENDS_DELAY=1
REDDIT_DELAY=1
GITHUB_DELAY=1
```

### Production Environment

For production deployment, set these variables in your deployment platform:
- **DATABASE_URL**: Your Supabase connection string
- **NODE_ENV**: Set to "production"

## Deployment Options

### 1. Vercel (Recommended)

**Pros**: Free tier, easy setup, built-in cron jobs
**Cons**: Limited execution time (30 seconds)

#### Setup:
1. **Deploy to Vercel**:
   ```bash
   vercel
   ```

2. **Set environment variables**:
   ```bash
   vercel env add DATABASE_URL
   ```

3. **Configure cron schedule** in `vercel.json`:
   ```json
   {
     "crons": [
       {
         "path": "/api/cron/trending",
         "schedule": "0 0 * * *"
       }
     ]
   }
   ```

4. **Create API endpoint** (`api/cron/trending.js`):
   ```javascript
   export default async function handler(req, res) {
     if (req.method !== 'POST') {
       return res.status(405).json({ error: 'Method not allowed' });
     }
     
     try {
       const { fetchAndStoreTrendingKeywords } = require('../../cron-job/trending.js');
       const result = await fetchAndStoreTrendingKeywords();
       
       return res.status(200).json(result);
     } catch (error) {
       return res.status(500).json({ error: error.message });
     }
   }
   ```

### 2. GitHub Actions

**Pros**: Free for public repos, good logging, reliable
**Cons**: Requires GitHub repository

#### Setup:
1. **Create workflow file** (`.github/workflows/trending-cron.yml`):
   ```yaml
   name: Trending Keywords Cron
   
   on:
     schedule:
       - cron: '0 0 * * *'
     workflow_dispatch:
   
   jobs:
     fetch-trending:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v2
         - uses: actions/setup-node@v2
           with:
             node-version: '18'
         - run: |
             cd cron-job
             npm install
             npm start
           env:
             DATABASE_URL: ${{ secrets.DATABASE_URL }}
   ```

2. **Add secret** in GitHub repository settings:
   - Go to Settings → Secrets and variables → Actions
   - Add `DATABASE_URL` secret

### 3. Railway

**Pros**: Easy deployment, good free tier, supports cron jobs
**Cons**: Limited free tier

#### Setup:
1. **Connect repository** to Railway
2. **Set environment variables**:
   - `DATABASE_URL`
   - `NODE_ENV=production`
3. **Deploy** automatically

### 4. Render

**Pros**: Good free tier, reliable
**Cons**: Services sleep after inactivity

#### Setup:
1. **Create new Web Service** in Render
2. **Connect repository**
3. **Set build command**: `cd cron-job && npm install`
4. **Set start command**: `cd cron-job && npm start`
5. **Add environment variables**:
   - `DATABASE_URL`
   - `NODE_ENV=production`

### 5. External Cron Services

#### cron-job.org
1. Go to [cron-job.org](https://cron-job.org)
2. Create new cron job:
   - **URL**: `https://your-api.com/trigger`
   - **Method**: `POST`
   - **Schedule**: `0 0 * * *`

#### EasyCron
1. Go to [EasyCron](https://www.easycron.com)
2. Create new cron job:
   - **URL**: `https://your-api.com/trigger`
   - **Method**: `POST`
   - **Schedule**: `0 0 * * *`

### 6. Server Crontab

**Pros**: Full control, no external dependencies
**Cons**: Requires server maintenance

#### Setup:
1. **Deploy the API server**:
   ```bash
   cd cron-job
   npm install
   npm run api
   ```

2. **Add to crontab**:
   ```bash
   crontab -e
   # Add this line:
   0 0 * * * curl -X POST https://your-api.com/trigger
   ```

### 7. Docker

**Pros**: Consistent environment, easy deployment
**Cons**: More complex setup

#### Setup:
1. **Create Dockerfile**:
   ```dockerfile
   FROM node:18-alpine
   WORKDIR /app
   COPY package*.json ./
   RUN npm install
   COPY . .
   CMD ["npm", "start"]
   ```

2. **Build and run**:
   ```bash
   docker build -t trending-cron .
   docker run -e DATABASE_URL=your-url trending-cron
   ```

## Environment Variables

### Required
- `DATABASE_URL`: PostgreSQL connection string

### Optional
- `NODE_ENV`: `development` or `production`
- `PORT`: Port for API server (default: 3001)
- `LOG_LEVEL`: Logging level (default: info)

## Testing

### Local Testing
```bash
# Test database connection
npm test

# Run manually
npm start

# Start API server
npm run api

# Start scheduler (for testing)
npm run scheduler
```

### API Testing
```bash
# Health check
curl http://localhost:3001/health

# Manual trigger
curl -X POST http://localhost:3001/trigger

# Status check
curl http://localhost:3001/status
```

## Monitoring

### Logs
The cron job provides detailed logging:
- ✅ Success messages
- ❌ Error messages
- 📊 Processing statistics
- 🗑️ Cleanup information

### Health Checks
```bash
# Test the cron job
npm test

# Check API health
curl https://your-api.com/health

# Check status
curl https://your-api.com/status
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check `DATABASE_URL` format
   - Verify database is accessible
   - Check SSL settings for production

2. **Table Not Found**
   - Run database migrations first
   - Check table name: `trendingkeyword`

3. **Permission Denied**
   - Check database user permissions
   - Verify connection string

4. **Timeout Issues (Vercel)**
   - Optimize the code for faster execution
   - Consider using external cron services

### Debug Commands

```bash
# Test database connection
node -e "require('./trending.js').fetchAndStoreTrendingKeywords()"

# Check environment variables
node -e "console.log(process.env.DATABASE_URL)"

# Run with verbose logging
DEBUG=* npm start
```

## Cost Comparison

| Platform | Free Tier | Paid Tier | Pros | Cons |
|----------|-----------|-----------|------|------|
| Vercel | ✅ 2 cron jobs | $20/month | Easy setup, reliable | 30s timeout |
| GitHub Actions | ✅ Unlimited | Free | Good logging, reliable | Requires repo |
| Railway | ✅ $5 credit | $5/month | Easy deployment | Limited free tier |
| Render | ✅ 750 hours | $7/month | Reliable | Services sleep |
| cron-job.org | ✅ 5 jobs | $3/month | Simple setup | Basic features |
| Server | ❌ | Varies | Full control | Maintenance required |

## Recommendation

For most use cases, I recommend:

1. **Vercel** - Best for simple setups with free tier
2. **GitHub Actions** - Best for developers with GitHub repos
3. **Railway** - Best for easy deployment with good features

---

**Status**: ✅ Ready for deployment
**Last Updated**: 2024-08-23
