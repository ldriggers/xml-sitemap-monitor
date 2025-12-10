# XML Sitemap Monitor - Roadmap

---
type: planning
status: active
maturity: seedling
created: 2025-12-10
updated: 2025-12-10
---

## Current State (v1.0)

### ✅ Completed

**Phase 1: Core Infrastructure**
- XML sitemap parsing (index + urlset)
- Change detection (discovered/modified/removed)
- Monthly CSV file partitioning
- Per-domain data folders

**Phase 2: Data Quality**
- Renamed terminology: `new`→`discovered`, `updated`→`modified`
- Added `first_seen_at` and `last_seen_at` columns
- CSV schema migration (auto-update headers)
- Deduplicated excessive "removed" entries

**Phase 3: Status Checking**
- HEAD request status checking
- Response header capture (ETag, Cache-Control, X-Robots-Tag)
- Inferred status fields (crawlable, indexable_from_head)
- GET content checking with SEO metadata extraction

**Phase 4: Stealth & Bypass**
- Created `10.07-seo-intel-common` shared library
- StealthFetcher with 5 browser profiles
- 6 referrer strategies
- Strategy persistence per domain

**Phase 5: Testing & Automation**
- Smoke tests (22 tests, <2s)
- Live tests (12 tests, ~30s)
- GitHub Actions workflows
- PowerShell automation script

---

## Upcoming (v1.1)

### 🔄 In Progress

- [ ] Integrate StealthFetcher as fallback in main pipeline
- [ ] Fix RocketMortgage sitemap URL in config
- [ ] Push to GitHub and verify Actions

### 📋 Planned

**Integration**
- [ ] Content check integration in main pipeline
- [ ] Stealth cascade: sitemap → HEAD → GET

**Monitoring**
- [ ] Add more competitors (CreditKarma, LendingTree)
- [ ] Mobile/News/Video sitemap tracking

---

## Future (v2.0+)

### 10.08 - Robots.txt Monitor
- Track disallow/crawl-delay changes
- Alert on significant changes

### 10.09 - Date Extractor
- Reconcile sitemap vs published dates
- Detect stale content

### 10.10 - Content Differ
- Hash + diff for changed pages
- Semantic change detection

### Data Pipeline
- BigQuery export
- Dashboard visualization
- Trend analysis

### Notifications
- Slack/email alerts
- Daily digest reports

---

## Architecture Vision

```
┌─────────────────────────────────────────────────────────────┐
│                    SEO INTEL PLATFORM                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Sitemap    │  │  Robots.txt  │  │    Date      │       │
│  │   Monitor    │  │   Monitor    │  │  Extractor   │       │
│  │   (10.06)    │  │   (10.08)    │  │   (10.09)    │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                │
│         └─────────────────┼─────────────────┘                │
│                           │                                  │
│                    ┌──────▼───────┐                          │
│                    │  SEO Intel   │                          │
│                    │   Common     │                          │
│                    │   (10.07)    │                          │
│                    └──────┬───────┘                          │
│                           │                                  │
│                    ┌──────▼───────┐                          │
│                    │   BigQuery   │                          │
│                    │   Export     │                          │
│                    └──────┬───────┘                          │
│                           │                                  │
│                    ┌──────▼───────┐                          │
│                    │  Dashboard   │                          │
│                    │   (Looker)   │                          │
│                    └──────────────┘                          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

