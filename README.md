# Python Cron Job for Trending Keywords

This Python-based cron job fetches trending data from Google Trends (using pytrends) and Wikipedia API, then stores it in the PostgreSQL database.

## 🚀 Features

- **Google Trends Integration**: Uses pytrends library for real trending searches
- **Wikipedia API**: Fetches trending topics from Wikipedia
- **Database Integration**: Direct PostgreSQL storage
- **Error Handling**: Robust error handling and logging
- **Smart Categorization**: Automatically categorizes keywords

## 📦 Installation

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your DATABASE_URL
   ```

## 🧪 Testing

Run the test suite to verify everything works:

```bash
python test.py
```

## 🏃‍♂️ Usage

### Manual Run
```bash
python trending.py
```

## 📊 Data Sources

- **Google Trends**: Real trending searches via pytrends library
- **Wikipedia**: Trending topics from Wikipedia API

## 🔧 Configuration

See `env.example` for all available environment variables.

## 🚀 Deployment

See `DEPLOYMENT.md` for detailed deployment instructions.

## 📝 Logs

The cron job provides detailed logging:
- ✅ Success operations
- ❌ Error operations
- 📊 Data fetching
- 🗑️ Data cleanup
- 🔄 Processing status

## 🔧 Troubleshooting

### Pytrends Issues
If you get `method_whitelist` errors:
```bash
pip uninstall pytrends requests urllib3 -y
pip install requests==2.31.0 urllib3==1.26.18 pytrends==4.9.2
```

### Database Connection
Make sure your `DATABASE_URL` is correctly formatted:
```
postgresql://postgres.project-ref:password@aws-0-region.pooler.supabase.com:6543/postgres
```
