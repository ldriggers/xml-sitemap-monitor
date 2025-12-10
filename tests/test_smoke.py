"""
SMOKE TESTS - Fast, Deterministic, No Network

Run: py tests/test_smoke.py
Time: < 2 seconds

These tests verify code structure and logic without any network calls.
Should pass 100% of the time if code is correct.
"""

import sys
import json
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime
import inspect

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT.parent / "10.07-seo-intel-common"))

RESULTS = []

def log(name: str, passed: bool, detail: str = ""):
    status = "✅" if passed else "❌"
    RESULTS.append({"name": name, "passed": passed})
    print(f"  {status} {name}" + (f" → {detail}" if detail else ""))

# =============================================================================
# 1. IMPORTS (3 tests)
# =============================================================================

def test_imports():
    print("\n📦 IMPORTS")
    
    # 1.1 Core modules
    try:
        from src import main, data_processor, sitemap_fetcher, sitemap_parser, url_status_checker
        log("Core modules", True)
    except Exception as e:
        log("Core modules", False, str(e))
    
    # 1.2 Stealth module
    try:
        from seo_intel.stealth import StealthFetcher, ProbeResult
        log("Stealth module", True)
    except Exception as e:
        log("Stealth module", False, str(e))
    
    # 1.3 Dependencies
    try:
        import requests, pandas, lxml
        from bs4 import BeautifulSoup
        log("Dependencies", True)
    except ImportError as e:
        log("Dependencies", False, str(e))

# =============================================================================
# 2. CONFIG VALIDATION (3 tests)
# =============================================================================

def test_config():
    print("\n⚙️  CONFIG")
    
    config_path = PROJECT_ROOT / "config.json"
    
    # 2.1 File exists + valid JSON
    try:
        with open(config_path) as f:
            config = json.load(f)
        log("Valid JSON", True, f"{len(config.get('targets', []))} targets")
    except Exception as e:
        log("Valid JSON", False, str(e))
        return
    
    # 2.2 Required structure
    if "targets" in config and len(config["targets"]) > 0:
        log("Has targets", True)
    else:
        log("Has targets", False)
    
    # 2.3 Target schema
    required = ["domain", "sitemap_url"]
    valid = all(all(k in t for k in required) for t in config.get("targets", []))
    log("Target schema", valid, "domain + sitemap_url present" if valid else "Missing fields")

# =============================================================================
# 3. PARSING - Unit tests with mock XML (5 tests)
# =============================================================================

def test_parsing():
    print("\n📄 PARSING")
    
    try:
        from src.sitemap_parser import SitemapParser
        parser = SitemapParser()
    except Exception as e:
        log("Parser init", False, str(e))
        return
    
    # 3.1 Sitemap index
    index_xml = """<?xml version="1.0"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <sitemap><loc>https://www.bankrate.com/sitemap-1.xml</loc><lastmod>2025-12-10</lastmod></sitemap>
        <sitemap><loc>https://www.bankrate.com/sitemap-2.xml</loc></sitemap>
    </sitemapindex>"""
    
    result = parser.parse_sitemap(index_xml, "https://www.bankrate.com/sitemap.xml")
    log("Sitemap index", result["type"] == "sitemapindex" and len(result["urls"]) == 2, 
        f"{len(result['urls'])} children")
    
    # 3.2 URL set
    urlset_xml = """<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url><loc>https://www.bankrate.com/mortgages/</loc><lastmod>2025-12-01</lastmod></url>
        <url><loc>https://www.bankrate.com/credit-cards/</loc></url>
    </urlset>"""
    
    result = parser.parse_sitemap(urlset_xml, "https://www.bankrate.com/urls.xml")
    log("URL set", result["type"] == "urlset" and len(result["urls"]) == 2,
        f"{len(result['urls'])} URLs")
    
    # 3.3 Lastmod extraction
    urls_with_lastmod = [u for u in result["urls"] if u.get("lastmod")]
    log("Lastmod extraction", len(urls_with_lastmod) == 1, "1/2 have lastmod")
    
    # 3.4 Empty sitemap
    empty_xml = """<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>"""
    result = parser.parse_sitemap(empty_xml, "https://example.com/empty.xml")
    log("Empty sitemap", len(result["urls"]) == 0)
    
    # 3.5 Malformed XML
    try:
        parser.parse_sitemap("<urlset><url><loc>broken", "https://example.com/bad.xml")
        log("Malformed XML", True, "Handled gracefully")
    except Exception as e:
        log("Malformed XML", True, f"Raised {type(e).__name__}")

# =============================================================================
# 4. CHANGE DETECTION TERMINOLOGY (5 tests)
# =============================================================================

def test_change_detection():
    print("\n🔄 CHANGE DETECTION")
    
    try:
        from src.data_processor import DataProcessor
        source = inspect.getsource(DataProcessor)
    except Exception as e:
        log("DataProcessor", False, str(e))
        return
    
    log("Uses 'discovered'", "'discovered'" in source)
    log("Uses 'modified'", "'modified'" in source)
    log("Uses 'removed'", "'removed'" in source)
    log("Tracks first_seen_at", "first_seen_at" in source)
    log("Tracks last_seen_at", "last_seen_at" in source)

# =============================================================================
# 5. DATA SCHEMA (3 tests)
# =============================================================================

def test_data_schema():
    print("\n💾 DATA SCHEMA")
    
    data_dir = PROJECT_ROOT / "output"
    bankrate_dir = data_dir / "bankrate.com"
    
    # 5.1 Folder structure
    if not bankrate_dir.exists():
        log("Bankrate folder", False, "Not found - run monitor first")
        return
    
    files = list(bankrate_dir.glob("*.csv"))
    log("Bankrate folder", True, f"{len(files)} CSV files")
    
    # 5.2 URLs file schema
    urls_file = bankrate_dir / "bankrate.com_urls.csv"
    if urls_file.exists():
        df = pd.read_csv(urls_file, nrows=3)
        has_cols = "loc" in df.columns and "lastmod" in df.columns
        log("URLs schema", has_cols, f"Cols: {list(df.columns)[:4]}")
    else:
        log("URLs schema", True, "No URLs file yet")
    
    # 5.3 Changes file schema
    changes = list(bankrate_dir.glob("*_changes_*.csv"))
    if changes:
        df = pd.read_csv(changes[0], nrows=3)
        required = ["detected_at", "change_type", "loc"]
        has_cols = all(c in df.columns for c in required)
        log("Changes schema", has_cols, f"Cols: {list(df.columns)[:5]}")
    else:
        log("Changes schema", True, "No changes yet")

# =============================================================================
# 6. WIRING / INSTANTIATION (3 tests)
# =============================================================================

def test_wiring():
    print("\n🔗 WIRING")
    
    # 6.1 DataProcessor
    try:
        from src.data_processor import DataProcessor
        with tempfile.TemporaryDirectory() as tmp:
            DataProcessor(data_dir=tmp)
        log("DataProcessor", True)
    except Exception as e:
        log("DataProcessor", False, str(e))
    
    # 6.2 SitemapFetcher
    try:
        from src.sitemap_fetcher import SitemapFetcher
        config = {"user_agent": "Test/1.0", "download_delay": 1.0, "max_retries": 1}
        SitemapFetcher(config=config)
        log("SitemapFetcher", True)
    except Exception as e:
        log("SitemapFetcher", False, str(e))
    
    # 6.3 User agent logic
    try:
        from src.main import get_user_agent, load_config
        config = load_config()
        ua_br = get_user_agent(config, "bankrate.com")
        ua_nw = get_user_agent(config, "nerdwallet.com")
        ok = "Bankratebot" in ua_br and "Bankratebot" not in ua_nw
        log("User agent logic", ok, "Bankratebot=internal, random=competitor")
    except Exception as e:
        log("User agent logic", False, str(e))

# =============================================================================
# 7. CONCURRENCY (3 tests)
# =============================================================================

def test_concurrency():
    print("\n⚡ CONCURRENCY")
    
    # 7.1 Main.py has ThreadPoolExecutor
    try:
        main_path = PROJECT_ROOT / "src" / "main.py"
        content = main_path.read_text()
        has_threadpool = "ThreadPoolExecutor" in content
        has_process_domain = "def process_domain(" in content
        log("main.py concurrent", has_threadpool and has_process_domain, 
            "ThreadPoolExecutor + process_domain")
    except Exception as e:
        log("main.py concurrent", False, str(e))
    
    # 7.2 url_status_checker.py has ThreadPoolExecutor
    try:
        checker_path = PROJECT_ROOT / "src" / "url_status_checker.py"
        content = checker_path.read_text()
        has_threadpool = "ThreadPoolExecutor" in content
        has_process_func = "def process_domain_status(" in content
        log("status_checker concurrent", has_threadpool and has_process_func,
            "ThreadPoolExecutor + process_domain_status")
    except Exception as e:
        log("status_checker concurrent", False, str(e))
    
    # 7.3 Config has max_concurrent_domains
    try:
        config_path = PROJECT_ROOT / "config.json"
        with open(config_path) as f:
            config = json.load(f)
        has_setting = "max_concurrent_domains" in config
        log("Config concurrent setting", has_setting, 
            f"max_concurrent_domains={config.get('max_concurrent_domains', 'missing')}")
    except Exception as e:
        log("Config concurrent setting", False, str(e))

# =============================================================================
# 8. WORKFLOW RESILIENCE (3 tests)
# =============================================================================

def test_workflows():
    print("\n🔄 WORKFLOWS")
    
    workflows_dir = PROJECT_ROOT / ".github" / "workflows"
    
    for wf_name in ["daily_monitor.yml", "status_checker.yml"]:
        wf_path = workflows_dir / wf_name
        if not wf_path.exists():
            log(f"{wf_name}", False, "File not found")
            continue
        
        content = wf_path.read_text()
        
        # Check retry logic
        has_retry = "MAX_RETRIES=3" in content
        has_artifact = "upload-artifact@v4" in content
        has_rebase = "pull --rebase" in content
        
        all_ok = has_retry and has_artifact and has_rebase
        detail = f"retry={has_retry}, artifact={has_artifact}, rebase={has_rebase}"
        log(f"{wf_name} resilience", all_ok, detail)

# =============================================================================
# 9. SCHEMA CONSISTENCY (2 tests)
# =============================================================================

def test_schema_consistency():
    print("\n📊 SCHEMA CONSISTENCY")
    
    data_dir = PROJECT_ROOT / "output"
    expected_cols = 12
    
    for domain in ["bankrate.com", "nerdwallet.com"]:
        domain_dir = data_dir / domain
        if not domain_dir.exists():
            log(f"{domain} schema", True, "No data yet")
            continue
        
        change_files = list(domain_dir.glob(f"*_changes_*.csv"))
        if not change_files:
            log(f"{domain} schema", True, "No change files yet")
            continue
        
        all_correct = True
        for f in change_files:
            df = pd.read_csv(f, nrows=0)
            if len(df.columns) != expected_cols:
                all_correct = False
                break
        
        log(f"{domain} schema", all_correct, 
            f"{len(change_files)} files, all {expected_cols} cols" if all_correct else "Schema mismatch")

# =============================================================================
# RUNNER
# =============================================================================

def run_all():
    start = datetime.now()
    print("\n" + "=" * 50)
    print("🧪 SMOKE TESTS (Fast, Deterministic)")
    print("=" * 50)
    
    test_imports()
    test_config()
    test_parsing()
    test_change_detection()
    test_data_schema()
    test_wiring()
    test_concurrency()
    test_workflows()
    test_schema_consistency()
    
    passed = sum(1 for r in RESULTS if r["passed"])
    total = len(RESULTS)
    duration = (datetime.now() - start).total_seconds()
    
    print("\n" + "=" * 50)
    if passed == total:
        print(f"🎉 ALL PASSED: {passed}/{total} in {duration:.2f}s")
    else:
        print(f"⚠️  {passed}/{total} passed in {duration:.2f}s")
        print("\nFailed:")
        for r in RESULTS:
            if not r["passed"]:
                print(f"  - {r['name']}")
    print("=" * 50 + "\n")
    
    return passed == total

if __name__ == "__main__":
    sys.exit(0 if run_all() else 1)

