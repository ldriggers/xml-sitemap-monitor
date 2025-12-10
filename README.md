# XML Sitemap Monitor

A lightweight business intelligence system that tracks content changes across competitor websites by monitoring their XML sitemaps. Runs daily via GitHub Actions with zero infrastructure cost.

## Features

- **Sitemap Monitoring**: Fetches and parses XML sitemaps (index + urlset formats)
- **Change Detection**: Identifies discovered, modified, and removed URLs
- **URL Status Checking**: HEAD/GET requests to verify page availability and SEO signals
- **Concurrent Processing**: Parallel domain processing with configurable workers
- **Historical Tracking**: Monthly CSV partitions with `first_seen_at`/`last_seen_at`
- **Stealth Fetching**: Browser fingerprinting and referrer spoofing for blocking sites
- **Fault Tolerant**: Per-domain error handling, push retry with artifacts backup

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     GitHub Actions (Daily)                       │
├─────────────────────────────────────────────────────────────────┤
│  1. Sitemap Fetch    →    2. Status Check    →    3. Commit     │
│     (main.py)              (url_status_checker.py)   (git push)  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Data Storage (CSV)                          │
├─────────────────────────────────────────────────────────────────┤
│  data/                                                           │
│  ├── bankrate.com/                                               │
│  │   ├── bankrate.com_urls.csv           (current state)        │
│  │   ├── bankrate.com_changes_2025-12.csv (monthly changes)     │
│  │   └── bankrate.com_status_history_*.csv                      │
│  ├── nerdwallet.com/                                             │
│  ├── investopedia.com/                                           │
│  └── rocketmortgage.com/                                         │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Clone
git clone https://github.com/ldriggers/xml-sitemap-monitor.git
cd xml-sitemap-monitor

# Install
pip install -r requirements.txt

# Run sitemap monitor
python -m src.main

# Run status checker
python -m src.url_status_checker

# Run tests
python -m pytest tests/ -v
```

## Configuration

Edit `config.json`:

```json
{
  "targets": [
    {
      "domain": "bankrate.com",
      "sitemap_url": "https://www.bankrate.com/sitemap/sitemap-index.xml",
      "download_delay": 1.5,
      "fetch_timeout": 30,
      "status_check": {
        "enabled": true,
        "check_new": true,
        "check_updated": true,
        "check_removed": true,
        "max_per_run": 100
      }
    }
  ],
  "max_concurrent_domains": 4,
  "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.1)"
}
```

## Data Schema

### Changes CSV (12 columns)
| Column | Description |
|--------|-------------|
| `detected_at` | UTC timestamp of detection |
| `domain` | Target domain |
| `loc` | URL |
| `change_type` | discovered / modified / removed |
| `first_seen_at` | First detection timestamp |
| `last_seen_at` | Most recent detection |
| `lastmod` | Sitemap lastmod value |
| `lastmod_prev` | Previous lastmod (for modified) |
| `sitemap_source_url` | Source sitemap URL |
| `section` | URL path section |
| `subsection` | URL path subsection |
| `path_depth` | URL path depth |

## GitHub Actions

### Workflows
- **Daily Sitemap Check** (`daily_monitor.yml`): Runs at midnight PT with 0-5hr jitter
- **URL Status Check** (`status_checker.yml`): Triggers after sitemap check completes

### Resilience Features
- Push retry with pull-rebase (3 attempts)
- Artifact backup before push (7-day retention)
- Per-domain fault tolerance

## Testing

```bash
# Fast smoke tests (29 tests, ~2s)
python tests/test_smoke.py

# Full suite including live tests (33 tests, ~5s)
python -m pytest tests/ -v
```

## Project Structure

```
├── .github/workflows/
│   ├── daily_monitor.yml
│   └── status_checker.yml
├── src/
│   ├── main.py              # Sitemap fetching orchestrator
│   ├── sitemap_fetcher.py   # HTTP fetching with retry
│   ├── sitemap_parser.py    # XML parsing
│   ├── data_processor.py    # Change detection & storage
│   └── url_status_checker.py # HEAD/GET status checking
├── tests/
│   ├── test_smoke.py        # Fast deterministic tests
│   └── test_live.py         # Network-dependent tests
├── data/                    # CSV data files (gitignored for large files)
├── config.json
├── requirements.txt
└── README.md
```

## Links

- **GitHub**: https://github.com/ldriggers/xml-sitemap-monitor
- **Actions**: https://github.com/ldriggers/xml-sitemap-monitor/actions
- **Shared Library**: `10.07-seo-intel-common` (stealth fetching)
