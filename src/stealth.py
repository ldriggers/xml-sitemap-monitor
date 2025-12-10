"""
Stealth Fetcher - Creative 403/402 bypass strategies

When a sitemap returns 403 Forbidden or 402 Payment Required, 
this module tries multiple stealth strategies to successfully fetch it.

Strategies (in order of attempt):
1. Full Chrome header impersonation (exact header order)
2. Referrer spoofing (pretend to come from Google)
3. Accept-Encoding variations (some sites require specific encoding)
4. curl_cffi TLS fingerprint impersonation (if available)
5. Different browser profiles (Firefox, Safari, Edge)

Usage:
    from src.stealth import StealthFetcher, ProbeResult
    
    fetcher = StealthFetcher()
    result = fetcher.fetch("https://www.example.com/sitemap.xml")
    
    if result.success:
        print(f"Strategy '{result.strategy}' worked!")
        print(result.content)

Strategy History:
    The probe persists successful/failed strategies to a JSON file
    so we can:
    - Skip known-failing strategies
    - Start with known-working strategies
    - Track when strategies stop working (for escalation)

NOTE: This is a local copy from seo-intel-common for self-contained deployment.
When seo-intel-common is published, prefer importing from there.
"""

import json
import logging
import os
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from urllib.parse import urlparse
from pathlib import Path

logger = logging.getLogger(__name__)

# Default location for strategy history (relative to project root)
DEFAULT_STRATEGY_HISTORY_PATH = "output/stealth_strategy_history.json"

@dataclass
class ProbeResult:
    """Result of a probe attempt."""
    success: bool
    status_code: int
    strategy: str
    content: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    attempts: int = 0
    strategies_tried: Optional[List[str]] = None
    strategies_failed: Optional[List[str]] = None


@dataclass
class StrategyRecord:
    """Record of a strategy attempt for a domain."""
    domain: str
    strategy: str
    success: bool
    status_code: int
    timestamp: str
    url: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


# =============================================================================
# BROWSER PROFILES - Exact header fingerprints
# =============================================================================

CHROME_WINDOWS = {
    "name": "Chrome/Windows",
    "headers": {
        # Order matters! Some WAFs check header order
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
}

CHROME_MAC = {
    "name": "Chrome/Mac",
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
}

FIREFOX_WINDOWS = {
    "name": "Firefox/Windows",
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    }
}

SAFARI_MAC = {
    "name": "Safari/Mac",
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    }
}

EDGE_WINDOWS = {
    "name": "Edge/Windows",
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    }
}

BROWSER_PROFILES = [CHROME_WINDOWS, CHROME_MAC, FIREFOX_WINDOWS, SAFARI_MAC, EDGE_WINDOWS]


# =============================================================================
# REFERRER STRATEGIES
# =============================================================================

def get_referrer_strategies(url: str) -> List[Dict[str, str]]:
    """Generate referrer variations to try."""
    parsed = urlparse(url)
    domain = parsed.netloc
    
    return [
        {"name": "no_referrer", "Referer": None},
        {"name": "google_search", "Referer": f"https://www.google.com/search?q=site:{domain}+sitemap"},
        {"name": "google_direct", "Referer": "https://www.google.com/"},
        {"name": "self_referrer", "Referer": f"https://{domain}/"},
        {"name": "robots_txt", "Referer": f"https://{domain}/robots.txt"},
        {"name": "bing_search", "Referer": f"https://www.bing.com/search?q=site:{domain}"},
    ]


# =============================================================================
# PROBE CLASS
# =============================================================================

class StealthFetcher:
    """
    Intelligent sitemap fetcher that tries multiple strategies to bypass 403s.
    
    Persists strategy history to JSON for:
    - Starting with known-working strategies (faster)
    - Skipping known-failing strategies (efficient)
    - Tracking when strategies stop working (escalation)
    """
    
    def __init__(
        self, 
        timeout: int = 30, 
        max_retries: int = 2,
        history_path: Optional[str] = None
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.history_path = Path(history_path or DEFAULT_STRATEGY_HISTORY_PATH)
        self.history = self._load_history()
        
    def _load_history(self) -> Dict[str, Any]:
        """Load strategy history from disk."""
        if self.history_path.exists():
            try:
                with open(self.history_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load strategy history: {e}")
        return {"domains": {}, "records": []}
    
    def _save_history(self):
        """Save strategy history to disk."""
        try:
            os.makedirs(self.history_path.parent, exist_ok=True)
            with open(self.history_path, 'w') as f:
                json.dump(self.history, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save strategy history: {e}")
    
    def _record_attempt(self, domain: str, url: str, strategy: str, success: bool, status_code: int):
        """Record a strategy attempt for future reference."""
        record = StrategyRecord(
            domain=domain,
            strategy=strategy,
            success=success,
            status_code=status_code,
            timestamp=datetime.now(timezone.utc).isoformat(),
            url=url
        )
        
        # Update domain summary
        if domain not in self.history["domains"]:
            self.history["domains"][domain] = {
                "working_strategies": [],
                "failed_strategies": [],
                "last_success": None,
                "last_failure": None,
            }
        
        domain_info = self.history["domains"][domain]
        
        if success:
            if strategy not in domain_info["working_strategies"]:
                domain_info["working_strategies"].append(strategy)
            domain_info["last_success"] = record.timestamp
            # Remove from failed if it was there
            if strategy in domain_info["failed_strategies"]:
                domain_info["failed_strategies"].remove(strategy)
        else:
            if strategy not in domain_info["failed_strategies"]:
                domain_info["failed_strategies"].append(strategy)
            domain_info["last_failure"] = record.timestamp
        
        # Keep last 100 records per domain
        self.history["records"].append(record.to_dict())
        self.history["records"] = self.history["records"][-500:]  # Global cap
        
        self._save_history()
    
    def get_domain_status(self, domain: str) -> Dict[str, Any]:
        """Get strategy status for a domain."""
        return self.history["domains"].get(domain, {
            "working_strategies": [],
            "failed_strategies": [],
            "last_success": None,
            "last_failure": None,
        })
        
    def fetch(self, url: str, verbose: bool = True) -> ProbeResult:
        """
        Try multiple strategies to fetch a sitemap.
        
        Strategy order:
        1. Try known-working strategies first (from history)
        2. Try all other strategies, skipping known-failed ones
        3. Record all attempts for future runs
        
        Returns the first successful result, or the last failure.
        """
        import requests
        
        parsed = urlparse(url)
        domain = parsed.netloc
        attempts = 0
        strategies_tried = []
        strategies_failed = []
        
        # Get domain history
        domain_status = self.get_domain_status(domain)
        working = domain_status.get("working_strategies", [])
        failed = domain_status.get("failed_strategies", [])
        
        if working and verbose:
            logger.info(f"📚 {domain} has {len(working)} known-working strategies: {working}")
        if failed and verbose:
            logger.info(f"⏭️ Skipping {len(failed)} known-failed strategies")
        
        # Build strategy list: working first, then new ones, skip failed
        all_strategies = []
        
        for profile in BROWSER_PROFILES:
            for ref_strategy in get_referrer_strategies(url):
                strategy_name = f"{profile['name']}+{ref_strategy['name']}"
                all_strategies.append({
                    "name": strategy_name,
                    "profile": profile,
                    "referrer": ref_strategy,
                    "type": "browser"
                })
        
        # Add curl_cffi strategies
        for impersonate in ["chrome131", "chrome120", "safari17_0", "edge131"]:
            all_strategies.append({
                "name": f"curl_cffi/{impersonate}",
                "impersonate": impersonate,
                "type": "curl_cffi"
            })
        
        # Sort: working first, unknown middle, failed last (but we'll skip failed)
        def sort_key(s):
            if s["name"] in working:
                return 0  # Try first
            elif s["name"] in failed:
                return 2  # Skip (or try last)
            return 1  # Try middle
        
        all_strategies.sort(key=sort_key)
        
        # Try strategies
        for strat in all_strategies:
            strategy_name = strat["name"]
            
            # Skip known-failed strategies (unless we've exhausted others)
            if strategy_name in failed and attempts < len(all_strategies) - len(failed):
                continue
            
            attempts += 1
            strategies_tried.append(strategy_name)
            
            if verbose:
                priority = "🚀" if strategy_name in working else "🔍"
                logger.info(f"{priority} Trying: {strategy_name}")
            
            try:
                time.sleep(random.uniform(0.5, 1.5))
                
                if strat["type"] == "browser":
                    headers = strat["profile"]["headers"].copy()
                    if strat["referrer"].get("Referer"):
                        headers["Referer"] = strat["referrer"]["Referer"]
                    
                    response = requests.get(
                        url,
                        headers=headers,
                        timeout=self.timeout,
                        allow_redirects=True
                    )
                    
                elif strat["type"] == "curl_cffi":
                    try:
                        from curl_cffi import requests as curl_requests
                        response = curl_requests.get(
                            url,
                            impersonate=strat["impersonate"],
                            timeout=self.timeout
                        )
                    except ImportError:
                        if verbose:
                            logger.debug("curl_cffi not installed, skipping")
                        continue
                
                # Check result
                if response.status_code == 200:
                    if verbose:
                        logger.info(f"✅ SUCCESS with {strategy_name} after {attempts} attempts")
                    
                    # Record success
                    self._record_attempt(domain, url, strategy_name, True, 200)
                    
                    return ProbeResult(
                        success=True,
                        status_code=200,
                        strategy=strategy_name,
                        content=response.text,
                        headers=dict(response.headers),
                        attempts=attempts,
                        strategies_tried=strategies_tried,
                        strategies_failed=strategies_failed
                    )
                else:
                    strategies_failed.append(strategy_name)
                    self._record_attempt(domain, url, strategy_name, False, response.status_code)
                    if verbose:
                        logger.debug(f"❌ {response.status_code} with {strategy_name}")
                        
            except Exception as e:
                strategies_failed.append(strategy_name)
                self._record_attempt(domain, url, strategy_name, False, 0)
                if verbose:
                    logger.warning(f"Error with {strategy_name}: {e}")
                continue
        
        # All strategies failed
        logger.error(f"❌ All {attempts} strategies failed for {domain}")
        
        return ProbeResult(
            success=False,
            status_code=403,
            strategy="all_failed",
            error=f"All {attempts} probe strategies failed",
            attempts=attempts,
            strategies_tried=strategies_tried,
            strategies_failed=strategies_failed
        )
    
    def fetch_head(self, url: str, preferred_strategy: Optional[str] = None, verbose: bool = False) -> Dict:
        """
        HEAD request with stealth - for status checking.
        
        Args:
            url: URL to check
            preferred_strategy: Strategy that worked for sitemap (try first)
            verbose: Log attempts
            
        Returns dict with status_code, headers, strategy used.
        """
        import requests
        
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Build strategy order: preferred first, then known-working, then others
        domain_status = self.get_domain_status(domain)
        working = domain_status.get("working_strategies", [])
        
        strategies_to_try = []
        
        # 1. Preferred strategy from higher-level fetch (sitemap)
        if preferred_strategy:
            strategies_to_try.append(preferred_strategy)
        
        # 2. Known working strategies for this domain
        for s in working:
            if s not in strategies_to_try:
                strategies_to_try.append(s)
        
        # 3. Default Chrome/Windows as fallback
        if "Chrome/Windows+no_referrer" not in strategies_to_try:
            strategies_to_try.append("Chrome/Windows+no_referrer")
        
        for strategy_name in strategies_to_try[:3]:  # Try max 3
            profile, referrer = self._parse_strategy(strategy_name)
            if not profile:
                continue
            
            headers = profile["headers"].copy()
            if referrer:
                headers["Referer"] = referrer
            
            try:
                if verbose:
                    logger.info(f"HEAD {url} with {strategy_name}")
                
                response = requests.head(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                return {
                    "url": url,
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "strategy": strategy_name,
                    "final_url": response.url,
                    "success": response.status_code == 200
                }
                
            except Exception as e:
                if verbose:
                    logger.warning(f"HEAD failed with {strategy_name}: {e}")
                continue
        
        return {
            "url": url,
            "status_code": 0,
            "headers": {},
            "strategy": "all_failed",
            "error": "All strategies failed",
            "success": False
        }
    
    def fetch_content(self, url: str, preferred_strategy: Optional[str] = None, verbose: bool = False) -> Dict:
        """
        GET request with stealth - for content extraction.
        
        Args:
            url: URL to fetch
            preferred_strategy: Strategy that worked for HEAD (try first)
            verbose: Log attempts
            
        Returns dict with status_code, content, headers, strategy used.
        """
        import requests
        
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Build strategy order
        domain_status = self.get_domain_status(domain)
        working = domain_status.get("working_strategies", [])
        
        strategies_to_try = []
        
        if preferred_strategy:
            strategies_to_try.append(preferred_strategy)
        for s in working:
            if s not in strategies_to_try:
                strategies_to_try.append(s)
        if "Chrome/Windows+no_referrer" not in strategies_to_try:
            strategies_to_try.append("Chrome/Windows+no_referrer")
        
        for strategy_name in strategies_to_try[:3]:
            profile, referrer = self._parse_strategy(strategy_name)
            if not profile:
                continue
            
            headers = profile["headers"].copy()
            if referrer:
                headers["Referer"] = referrer
            
            try:
                if verbose:
                    logger.info(f"GET {url} with {strategy_name}")
                
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                return {
                    "url": url,
                    "status_code": response.status_code,
                    "content": response.text if response.status_code == 200 else None,
                    "headers": dict(response.headers),
                    "strategy": strategy_name,
                    "final_url": response.url,
                    "success": response.status_code == 200
                }
                
            except Exception as e:
                if verbose:
                    logger.warning(f"GET failed with {strategy_name}: {e}")
                continue
        
        return {
            "url": url,
            "status_code": 0,
            "content": None,
            "headers": {},
            "strategy": "all_failed",
            "error": "All strategies failed",
            "success": False
        }
    
    def _parse_strategy(self, strategy_name: str) -> tuple:
        """Parse strategy name into profile and referrer."""
        # Handle curl_cffi strategies (not supported for HEAD/GET yet)
        if strategy_name.startswith("curl_cffi/"):
            return None, None
        
        # Parse "Chrome/Windows+google_search" format
        parts = strategy_name.split("+")
        if len(parts) != 2:
            return None, None
        
        profile_name, ref_name = parts
        
        # Find profile
        profile = None
        for p in BROWSER_PROFILES:
            if p["name"] == profile_name:
                profile = p
                break
        
        if not profile:
            return None, None
        
        # Get referrer (simplified - just use domain-based)
        referrer = None
        if ref_name == "google_search":
            referrer = "https://www.google.com/"
        elif ref_name == "google_direct":
            referrer = "https://www.google.com/"
        elif ref_name == "bing_search":
            referrer = "https://www.bing.com/"
        # no_referrer, self_referrer, robots_txt = None
        
        return profile, referrer

