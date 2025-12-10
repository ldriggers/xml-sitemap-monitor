"""
ARCHIVED: 2025-12-10
Original comprehensive smoke test - replaced by test_smoke.py + test_live.py

This was the first iteration combining all tests in one file.
Split into deterministic (smoke) and network-dependent (live) tests.

Results from final run: 20/22 passed
- Fixed: DataProcessor signature (data_dir only, no domain)
- Fixed: SitemapFetcher signature (config dict, not kwargs)
"""

# Original test categories:
# 1. IMPORTS - Module loading
# 2. CONFIG - Configuration validation  
# 3. PARSING - XML sitemap parsing (unit tests)
# 4. CHANGE DETECTION - Terminology validation
# 5. DATA OUTPUT - CSV schema validation
# 6. HTTP - Status/content checking (live)
# 7. STEALTH - Bypass strategies (live)
# 8. INTEGRATION - End-to-end wiring

# See test_smoke.py for deterministic tests (1-5, part of 8)
# See test_live.py for network tests (6-7, part of 8)

