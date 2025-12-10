# XML Sitemap Monitor

A lightweight business intelligence system that tracks content changes across competitor websites by monitoring their XML sitemaps. Runs daily via GitHub Actions with zero infrastructure cost.

## Features

- **Sitemap Monitoring**: Fetches and parses XML sitemaps (index + urlset formats)
- **Multi-Sitemap Support**: Configure multiple `sitemap_urls` per domain
- **Change Detection**: Identifies discovered, modified, and removed URLs
- **URL Status Checking**: HEAD/GET requests to verify page availability and SEO signals
- **Concurrent Processing**: Parallel domain processing with configurable workers
- **Historical Tracking**: Monthly CSV partitions with `first_seen_at`/`last_seen_at`
- **Stealth Fetching**: Browser fingerprinting and referrer spoofing for 403/402 fallback
- **Robots.txt Compliance**: Filters bot user agents by robots.txt rules
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
│  output/                                                         │
│  ├── bankrate.com/           (10,842 URLs)                      │
│  │   ├── bankrate.com_urls.csv           (current state)        │
│  │   ├── bankrate.com_changes_2025-12.csv (monthly changes)     │
│  │   └── bankrate.com_status_history_*.csv                      │
│  ├── nerdwallet.com/         (9,662 URLs)                       │
│  ├── investopedia.com/       (51,508 URLs)                      │
│  └── rocketmortgage.com/     (2,311 URLs)                       │
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
    },
    {
      "domain": "rocketmortgage.com",
      "sitemap_urls": [
        "https://www.rocketmortgage.com/sitemap.xml",
        "https://www.rocketmortgage.com/learn/sitemap.xml"
      ],
      "download_delay": 5.0
    }
  ],
  "max_concurrent_domains": 4,
  "data_directory": "output"
}
```

**Note**: Use `sitemap_url` (string) for single sitemaps, `sitemap_urls` (array) for multiple.

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
│   ├── daily_monitor.yml      # Midnight PT + 0-5hr jitter
│   └── status_checker.yml     # Triggers after sitemap check
├── src/
│   ├── main.py                # Sitemap fetching orchestrator
│   ├── sitemap_fetcher.py     # HTTP fetching with stealth fallback
│   ├── sitemap_parser.py      # XML parsing (index + urlset)
│   ├── data_processor.py      # Change detection & storage
│   ├── url_status_checker.py  # HEAD/GET status checking
│   ├── robots_checker.py      # Robots.txt parsing & UA filtering
│   ├── stealth.py             # StealthFetcher for 403 bypass
│   └── config.py              # Config loading & validation
├── tests/
│   ├── test_smoke.py          # 29 fast deterministic tests
│   └── test_live.py           # 4 network-dependent tests
├── output/                    # Per-domain CSV data
├── config.json
├── requirements.txt
└── README.md
```

## Links

- **GitHub**: https://github.com/ldriggers/xml-sitemap-monitor
- **Actions**: https://github.com/ldriggers/xml-sitemap-monitor/actions
- **Shared Library**: `10.07-seo-intel-common` (stealth fetching)
