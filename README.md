# Cron Job - Trending Keywords Data Fetcher

A Python-based cron job that fetches trending data from Google Trends and Wikipedia API, then stores it in the PostgreSQL database for the trending keywords dashboard.

## 🚀 Features

- **Google Trends Integration**: Uses pytrends library for real trending searches
- **Wikipedia API**: Fetches trending topics from Wikipedia
- **Database Integration**: Direct PostgreSQL storage via Supabase
- **Error Handling**: Robust error handling and logging
- **Smart Categorization**: Automatically categorizes keywords
- **Scheduled Execution**: Can be run manually or via GitHub Actions
- **Data Validation**: Validates data before database insertion

## 🛠️ Tech Stack

- **Language**: Python 3.11+
- **Data Sources**: Google Trends (pytrends), Wikipedia API
- **Database**: PostgreSQL (Supabase)
- **Deployment**: GitHub Actions (Scheduled Workflows)
- **Libraries**: pytrends, requests, psycopg2, python-dotenv

## 📋 Prerequisites

- Python 3.11+
- PostgreSQL database (Supabase)
- Google Trends access
- Wikipedia API access
- GitHub repository

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <your-repo>
cd cron-job

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy environment template
cp env.example .env

# Edit .env with your database credentials
DATABASE_URL="postgresql://postgres.project-ref:password@aws-0-region.pooler.supabase.com:6543/postgres"
```

### 3. Test the Script

```bash
# Run the test suite
python test.py

# Run the main script manually
python trending.py
```

## 📁 Project Structure

```
cron-job/
├── trending.py              # Main cron job script
├── test.py                  # Test suite
├── test-deployment.py       # Deployment test script
├── requirements.txt         # Python dependencies
├── .github/                 # GitHub Actions workflows
│   └── workflows/          # Workflow definitions
│       └── cron.yml        # Scheduled workflow
├── env.example             # Environment variables template
└── README.md               # This file
```

## 🔧 Configuration

### Environment Variables

#### Required
```bash
DATABASE_URL="postgresql://postgres.project-ref:password@aws-0-region.pooler.supabase.com:6543/postgres"
```

#### Optional
```bash
# For additional data sources
WIKIMEDIA_USER_AGENT="your_user_agent"
WIKIMEDIA_REFERER="your_referer"
```

### Database Connection

The script connects to your Supabase PostgreSQL database using the connection string format:
```
postgresql://postgres.project-ref:password@aws-0-region.pooler.supabase.com:6543/postgres
```

## 📊 Data Sources

### Google Trends
- **Library**: pytrends
- **Data**: Real trending searches
- **Categories**: Technology, Business, Entertainment, etc.
- **Frequency**: Daily trending topics

### Wikipedia
- **API**: Wikipedia Trending API
- **Data**: Trending articles and topics
- **Categories**: News, Events, People
- **Frequency**: Daily trending topics

## 🏃‍♂️ Usage

### Manual Execution
```bash
# Run the script manually
python trending.py
```

### Local Testing
```bash
# Test the script locally
python test.py

# Run with debug logging
python trending.py --debug
```

## 🚀 Deployment

### GitHub Actions (Recommended)

The cron job is deployed using GitHub Actions scheduled workflows.

#### 1. Repository Setup

1. **Push your code to GitHub**:
   ```bash
   git add .
   git commit -m "Add cron job with GitHub Actions"
   git push origin main
   ```

2. **Configure GitHub Secrets**:
   - Go to your GitHub repository
   - Navigate to **Settings** → **Secrets and variables** → **Actions**
   - Add the following secrets:
     - `DATABASE_URL`: Your PostgreSQL connection string
     - `WIKIMEDIA_USER_AGENT`: (Optional) Your user agent
     - `WIKIMEDIA_REFERER`: (Optional) Your referer

#### 2. Workflow Configuration

The workflow is defined in `.github/workflows/cron.yml`:

```yaml
name: Trending Keywords Cron Job

on:
  schedule:
    # Run daily at 2 AM UTC
    - cron: '0 2 * * *'
  workflow_dispatch: # Allow manual triggers

jobs:
  fetch-trending-data:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    
    - name: Run trending keywords script
      env:
        DATABASE_URL: ${{ secrets.DATABASE_URL }}
        WIKIMEDIA_USER_AGENT: ${{ secrets.WIKIMEDIA_USER_AGENT }}
        WIKIMEDIA_REFERER: ${{ secrets.WIKIMEDIA_REFERER }}
      run: |
        python trending.py
```

#### 3. Manual Execution

You can manually trigger the workflow:
- Go to **Actions** tab in your GitHub repository
- Select the "Trending Keywords Cron Job" workflow
- Click **Run workflow** → **Run workflow**

### Alternative: Manual Server Deployment

1. **Upload to Server**:
   ```bash
   scp -r cron-job/ user@server:/path/to/cron-job/
   ```

2. **Set up Cron**:
   ```bash
   crontab -e
   # Add: 0 2 * * * cd /path/to/cron-job && python trending.py
   ```

3. **Test Execution**:
   ```bash
   cd /path/to/cron-job
   python trending.py
   ```

## 📝 Logs and Monitoring

### GitHub Actions Logs
- **Workflow Runs**: Check the Actions tab in your GitHub repository
- **Job Logs**: Detailed logs for each execution
- **Error Tracking**: Failed runs are clearly marked
- **Success Tracking**: Successful runs with execution time

### Local Logs
The script provides detailed logging when run locally:
```
✅ Success operations
❌ Error operations
📊 Data fetching
🗑️ Data cleanup
🔄 Processing status
```

### Monitoring
- **GitHub Actions**: Monitor workflow execution in repository
- **Database Logs**: Check for data insertion
- **Email Notifications**: GitHub can send notifications on failures

## 🔧 Troubleshooting

### Common Issues

#### Pytrends Issues
If you get `method_whitelist` errors:
```bash
pip uninstall pytrends requests urllib3 -y
pip install requests==2.31.0 urllib3==1.26.18 pytrends==4.9.2
```

#### Database Connection
Make sure your `DATABASE_URL` is correctly formatted:
```
postgresql://postgres.project-ref:password@aws-0-region.pooler.supabase.com:6543/postgres
```

#### GitHub Actions Issues
- **Secret not found**: Ensure secrets are properly configured
- **Python version**: Check Python version in workflow
- **Dependencies**: Verify requirements.txt is up to date

#### Rate Limiting
- Google Trends has rate limits
- Wikipedia API has usage limits
- Implement delays between requests if needed

### Debug Mode
Enable debug logging by modifying the script:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🔒 Security

### Best Practices
- Use GitHub Secrets for sensitive data
- Implement proper error handling
- Validate data before database insertion
- Monitor for unusual activity

### Data Privacy
- No personal data is collected
- Only public trending data is fetched
- Data is stored securely in Supabase

## 🛠️ Development

### Adding New Data Sources
1. Create new function in `trending.py`
2. Add error handling
3. Update test suite
4. Document the new source

### Modifying Data Processing
1. Update categorization logic
2. Modify database insertion
3. Test with sample data
4. Update documentation

### Code Style
- Follow PEP 8
- Use type hints
- Document functions
- Write meaningful commit messages

## 📚 Documentation

- **Main Script**: `trending.py` - Contains all logic
- **Test Suite**: `test.py` - Comprehensive tests
- **Workflow**: `.github/workflows/cron.yml` - GitHub Actions configuration
- **Environment**: `env.example` - Environment variables

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
1. Check the GitHub Actions logs
2. Verify GitHub Secrets are configured
3. Test database connection
4. Create an issue with detailed information

## 🔗 Useful Links

- [pytrends Documentation](https://pypi.org/project/pytrends/)
- [Wikipedia API Documentation](https://www.mediawiki.org/wiki/API:Main_page)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Supabase Documentation](https://supabase.com/docs)

---

**Happy coding! 🎉**
