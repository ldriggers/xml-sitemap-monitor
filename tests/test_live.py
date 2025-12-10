"""
LIVE TESTS - Network-Dependent, Bankrate-focused

Run: py tests/test_live.py
Time: 30-60 seconds (network calls)

Uses bankrate.com exclusively - our own property, won't block us.
These tests verify actual HTTP behavior and stealth strategies.

Run manually or in nightly CI, not on every commit.
"""

import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT.parent / "10.07-seo-intel-common"))

RESULTS = []

def log(name: str, passed: bool, detail: str = ""):
    status = "✅" if passed else "❌"
    RESULTS.append({"name": name, "passed": passed})
    print(f"  {status} {name}" + (f" → {detail}" if detail else ""))

# =============================================================================
# 1. HEAD REQUEST TESTS (3 tests)
# =============================================================================

def test_head_requests():
    print("\n🌐 HEAD REQUESTS (bankrate.com)")
    
    try:
        from src.url_status_checker import check_url_head
    except ImportError as e:
        log("Import check_url_head", False, str(e))
        return
    
    # 1.1 Basic HEAD request
    try:
        result = check_url_head("https://www.bankrate.com/", timeout=15)
        status = result.get("status_code")
        log("HEAD status code", status == 200, f"Status: {status}")
    except Exception as e:
        log("HEAD status code", False, str(e))
        return
    
    # 1.2 Inferred fields
    has_inferred = all(k in result for k in ["inferred_crawlable", "inferred_indexable_from_head"])
    log("Inferred fields", has_inferred, 
        f"Crawlable: {result.get('inferred_crawlable')}")
    
    # 1.3 Header capture
    header_keys = [k for k in result.keys() if k.startswith("h_")]
    log("Header capture", len(header_keys) > 0, f"{len(header_keys)} headers captured")

# =============================================================================
# 2. CONTENT EXTRACTION (3 tests)
# =============================================================================

def test_content_extraction():
    print("\n📄 CONTENT EXTRACTION (bankrate.com)")
    
    try:
        from src.url_status_checker import check_url_content
    except ImportError as e:
        log("Import check_url_content", False, str(e))
        return
    
    # 2.1 GET request
    try:
        result = check_url_content("https://www.bankrate.com/", timeout=20)
        has_title = bool(result.get("c_title"))
        log("Title extraction", has_title, 
            f"'{result.get('c_title', '')[:50]}...'" if has_title else "No title")
    except Exception as e:
        log("Title extraction", False, str(e))
        return
    
    # 2.2 Canonical
    has_canonical = "c_canonical" in result
    log("Canonical extraction", has_canonical,
        result.get("c_canonical", "")[:60] if has_canonical else "Missing")
    
    # 2.3 Indexability inference
    has_indexable = "inferred_indexable" in result
    log("Indexability inference", has_indexable,
        f"Indexable: {result.get('inferred_indexable')}")

# =============================================================================
# 3. STEALTH FETCHER (4 tests)
# =============================================================================

def test_stealth():
    print("\n🥷 STEALTH FETCHER (bankrate.com)")
    
    try:
        from seo_intel.stealth import StealthFetcher
        fetcher = StealthFetcher(timeout=15)
    except Exception as e:
        log("StealthFetcher init", False, str(e))
        return
    
    # 3.1 Fetch sitemap
    try:
        result = fetcher.fetch("https://www.bankrate.com/sitemap/sitemap-index.xml", verbose=False)
        success = getattr(result, "success", False)
        strategy = getattr(result, "strategy", "unknown")
        log("Sitemap fetch", success, f"Strategy: {strategy}")
    except Exception as e:
        log("Sitemap fetch", False, str(e))
    
    # 3.2 fetch_head
    try:
        result = fetcher.fetch_head("https://www.bankrate.com/", verbose=False)
        success = result.get("success", False)
        log("fetch_head", success, f"Strategy: {result.get('strategy')}")
    except Exception as e:
        log("fetch_head", False, str(e))
    
    # 3.3 fetch_content
    try:
        result = fetcher.fetch_content("https://www.bankrate.com/", verbose=False)
        success = result.get("success", False) and result.get("content")
        size = len(result.get("content", "")) if success else 0
        log("fetch_content", success, f"{size:,} bytes")
    except Exception as e:
        log("fetch_content", False, str(e))
    
    # 3.4 Strategy inheritance (fetch sitemap, then use that strategy for HEAD)
    try:
        sitemap_result = fetcher.fetch("https://www.bankrate.com/sitemap/sitemap-index.xml", verbose=False)
        if getattr(sitemap_result, "success", False):
            working_strategy = getattr(sitemap_result, "strategy", None)
            head_result = fetcher.fetch_head(
                "https://www.bankrate.com/mortgages/",
                preferred_strategy=working_strategy,
                verbose=False
            )
            inherited = head_result.get("strategy") == working_strategy
            log("Strategy inheritance", head_result.get("success", False),
                "Used sitemap strategy" if inherited else f"Fell back to {head_result.get('strategy')}")
        else:
            log("Strategy inheritance", False, "Sitemap fetch failed first")
    except Exception as e:
        log("Strategy inheritance", False, str(e))

# =============================================================================
# 4. SITEMAP FETCHER (2 tests)
# =============================================================================

def test_sitemap_fetcher():
    print("\n📡 SITEMAP FETCHER (bankrate.com)")
    
    try:
        from src.sitemap_fetcher import SitemapFetcher
        config = {
            "user_agent": "Mozilla/5.0 (compatible; Bankratebot/1.0)",
            "download_delay": 1.0,
            "max_retries": 2
        }
        fetcher = SitemapFetcher(config=config)
    except Exception as e:
        log("SitemapFetcher init", False, str(e))
        return
    
    # 4.1 Fetch sitemap index
    content = None
    try:
        content = fetcher.fetch_sitemap_xml("https://www.bankrate.com/sitemap/sitemap-index.xml")
        has_content = content and len(content) > 100
        log("Fetch sitemap index", has_content, 
            f"{len(content):,} bytes" if content else "Empty")
    except Exception as e:
        log("Fetch sitemap index", False, str(e))
    
    # 4.2 Content is valid XML
    try:
        if content:
            is_xml = "<?xml" in content[:100] or "<sitemapindex" in content[:500]
            log("Valid XML response", is_xml)
        else:
            log("Valid XML response", False, "No content to check")
    except Exception as e:
        log("Valid XML response", False, str(e))

# =============================================================================
# RUNNER
# =============================================================================

def run_all():
    start = datetime.now()
    print("\n" + "=" * 50)
    print("🌐 LIVE TESTS (Network-Dependent)")
    print("   Target: bankrate.com")
    print("=" * 50)
    
    test_head_requests()
    test_content_extraction()
    test_stealth()
    test_sitemap_fetcher()
    
    passed = sum(1 for r in RESULTS if r["passed"])
    total = len(RESULTS)
    duration = (datetime.now() - start).total_seconds()
    
    print("\n" + "=" * 50)
    if passed == total:
        print(f"🎉 ALL PASSED: {passed}/{total} in {duration:.1f}s")
    else:
        print(f"⚠️  {passed}/{total} passed in {duration:.1f}s")
        print("\nFailed:")
        for r in RESULTS:
            if not r["passed"]:
                print(f"  - {r['name']}")
    print("=" * 50 + "\n")
    
    return passed == total

if __name__ == "__main__":
    sys.exit(0 if run_all() else 1)

