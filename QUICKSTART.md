# Quick Start Guide

---
type: reference
status: active
maturity: seedling
created: 2025-12-10
updated: 2025-12-10
---

## Prerequisites

- Python 3.10+
- pip

## Installation

```bash
# Clone the repository
git clone https://github.com/ldriggers/competitive_content_monitoring
cd competitive_content_monitoring

# Install dependencies
pip install -r requirements.txt

# Install shared library (optional, for stealth features)
cd ../10.07-seo-intel-common
pip install -e .
cd ../10.06-xml-sitemap-monitor
```

## Running Locally

### Option 1: PowerShell Script (Windows)

```powershell
# Show help
.\run.ps1 help

# Run smoke tests (fast, no network)
.\run.ps1 test

# Run live tests (network-dependent)
.\run.ps1 live

# Run sitemap monitor
.\run.ps1 monitor

# Run status checker
.\run.ps1 status

# Run both
.\run.ps1 all
```

### Option 2: Direct Python Commands

```bash
# Run sitemap monitor
py -m src.main

# Run status checker
py -m src.url_status_checker

# Run tests
py tests/test_smoke.py
py tests/test_live.py
```

## Configuration

Edit `config.json` to add/modify targets:

```json
{
  "targets": [
    {
      "domain": "example.com",
      "sitemap_url": "https://www.example.com/sitemap.xml",
      "download_delay": 2.0,
      "status_check": {
        "enabled": true,
        "check_new": true,
        "check_updated": true,
        "check_removed": true,
        "max_per_run": 100
      }
    }
  ]
}
```

## Output

Data is saved to `data/{domain}/`:

- `{domain}_urls.csv` - Current sitemap snapshot
- `{domain}_urls_all_time.csv` - All URLs ever seen
- `{domain}_changes_YYYY-MM.csv` - Monthly change log
- `{domain}_sitemaps.csv` - Sitemap file metadata
- `{domain}_status_history_YYYY-MM-DD.csv` - URL status checks

## GitHub Actions

The workflows run automatically:

1. **daily_monitor.yml** - Fetches sitemaps daily (staggered cron)
2. **status_checker.yml** - Checks URL status after sitemap run

To trigger manually: Actions → Select workflow → Run workflow

## Troubleshooting

### Import Errors

Make sure you're running from the project root:

```bash
cd 10.06-xml-sitemap-monitor
py -m src.main  # Not: py src/main.py
```

### Network Errors

Some competitors may block requests. The system will:
1. Retry with exponential backoff
2. Use stealth strategies if enabled
3. Log failures and continue

### Missing Data

First run creates empty data folders. Subsequent runs populate them.

