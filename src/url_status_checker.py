"""
1.0 URL Status Checker
Independent tool to check HTTP status of URLs from sitemap changes.

Key features:
- Runs SEPARATELY from sitemap monitor (different schedule)
- Checks new, updated, AND removed URLs
- HEAD requests only (lightweight, ~1-2KB per request)
- Captures full response headers for SEO signals:
  - X-Robots-Tag (noindex detection)
  - ETag (content change fingerprint)
  - Last-Modified, Cache-Control, Age
  - Content-Length (size changes)
  - Link (canonical, hreflang)
- Daily history tracking
- Circuit breaker: stops checking if too many failures
- Per-domain enable/disable in config

Usage:
    python -m src.url_status_checker
    python -m src.url_status_checker --domain bankrate.com
    python -m src.url_status_checker --check-type removed --limit 50
"""

import argparse
import logging
import os
import json
import random
import time
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse
from glob import glob

# Try to import StealthFetcher from shared library
try:
    from seo_intel.stealth import StealthFetcher, ProbeResult
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("status_checker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 1.1 Default settings
DEFAULT_TIMEOUT = 5  # Short timeout for HEAD requests
CONFIG_FILE = "config.json"

# 1.2 User agent configuration
# Bankratebot for our own properties, random bots for competitors
BANKRATE_USER_AGENT = "Mozilla/5.0 (compatible; Bankratebot/1.0; +https://www.bankrate.com)"
BANKRATE_DOMAINS = ["bankrate.com", "www.bankrate.com"]

# Bot user agents for competitor status checking
# Mix of AI bots and traditional search engine bots
COMPETITOR_BOT_USER_AGENTS = [
    # OpenAI bots
    "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)",
    "Mozilla/5.0 (compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot)",
    "Mozilla/5.0 (compatible; ChatGPT-User/1.0; +https://openai.com/chatgpt-user)",
    # Anthropic/Claude bots
    "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://www.anthropic.com/claude)",
    "Mozilla/5.0 (compatible; Claude-User/1.0; +https://www.anthropic.com)",
    # Perplexity
    "Mozilla/5.0 (compatible; PerplexityBot/1.0; +https://perplexity.ai/bot)",
    # Google bots
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    # Bing bots
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Mobile Safari/537.36 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    # Other search engines
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Mozilla/5.0 (compatible; DuckDuckBot/1.1; +http://duckduckgo.com/duckduckbot)",
    "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
    # SEO tools (commonly seen, legitimate)
    "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
    "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)",
]

DEFAULT_USER_AGENT = COMPETITOR_BOT_USER_AGENTS[0]


def get_user_agent_for_url(url: str) -> str:
    """
    1.3 Get appropriate user agent based on URL domain.
    
    - Bankrate URLs → Bankratebot (our own property)
    - Competitor URLs → Random bot from rotation
    """
    # Check if URL is a Bankrate property
    if any(bd in url for bd in BANKRATE_DOMAINS):
        return BANKRATE_USER_AGENT
    
    # Random bot for competitors
    return random.choice(COMPETITOR_BOT_USER_AGENTS)


def load_config() -> Dict:
    """Load configuration from config.json"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}


def get_domain_status_config(config: Dict, domain: str) -> Dict:
    """
    1.2 Get status check configuration for a domain.
    
    Config structure in config.json:
    {
        "targets": [
            {
                "domain": "bankrate.com",
                "status_check": {
                    "enabled": true,
                    "check_new": true,
                    "check_updated": true,
                    "check_removed": true,
                    "max_per_run": 100,
                    "failure_threshold": 0.5,
                    "backoff_days": 3,
                    "timeout": 5
                }
            }
        ]
    }
    """
    defaults = {
        "enabled": True,
        "check_new": True,
        "check_updated": True,
        "check_removed": True,
        "max_per_run": 100,
        "failure_threshold": 0.5,  # 50% failures triggers backoff
        "backoff_days": 3,
        "timeout": 5,
        "user_agent": DEFAULT_USER_AGENT,
    }
    
    for target in config.get("targets", []):
        if target.get("domain") == domain:
            domain_config = target.get("status_check", {})
            return {**defaults, **domain_config}
    
    return defaults


class CircuitBreaker:
    """
    2.0 Circuit Breaker for status checking.
    
    Tracks failure rates and prevents checking when too many failures occur.
    """
    
    def __init__(self, domain: str, data_dir: str = "data"):
        self.domain = domain
        self.data_dir = data_dir
        self.state_file = os.path.join(data_dir, domain, f"{domain}_status_check_state.json")
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load circuit breaker state from file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load circuit state: {e}")
        
        return {
            "is_open": False,
            "last_check": None,
            "last_failure_rate": 0,
            "consecutive_high_failures": 0,
            "backoff_until": None,
            "total_checks": 0,
            "total_failures": 0,
        }
    
    def _save_state(self):
        """Save circuit breaker state to file."""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def should_check(self, failure_threshold: float, backoff_days: int) -> bool:
        """
        Determine if we should proceed with status checks.
        
        Returns False if circuit is open (too many recent failures).
        """
        # Check if in backoff period
        if self.state.get("backoff_until"):
            backoff_until = datetime.fromisoformat(self.state["backoff_until"])
            if datetime.now(timezone.utc) < backoff_until:
                days_left = (backoff_until - datetime.now(timezone.utc)).days
                logger.warning(
                    f"Circuit OPEN for {self.domain}: backing off for {days_left} more days"
                )
                return False
            else:
                # Backoff expired, reset
                logger.info(f"Backoff expired for {self.domain}, resuming checks")
                self.state["is_open"] = False
                self.state["backoff_until"] = None
                self._save_state()
        
        return not self.state.get("is_open", False)
    
    def record_results(
        self, 
        total: int, 
        failures: int, 
        failure_threshold: float,
        backoff_days: int
    ):
        """
        Record check results and potentially trip the circuit.
        """
        if total == 0:
            return
        
        failure_rate = failures / total
        
        self.state["last_check"] = datetime.now(timezone.utc).isoformat()
        self.state["last_failure_rate"] = failure_rate
        self.state["total_checks"] += total
        self.state["total_failures"] += failures
        
        if failure_rate >= failure_threshold:
            self.state["consecutive_high_failures"] += 1
            
            # Trip circuit after 2 consecutive high-failure runs
            if self.state["consecutive_high_failures"] >= 2:
                self.state["is_open"] = True
                backoff_until = datetime.now(timezone.utc) + timedelta(days=backoff_days)
                self.state["backoff_until"] = backoff_until.isoformat()
                logger.warning(
                    f"Circuit TRIPPED for {self.domain}: "
                    f"{failure_rate:.0%} failure rate, backing off until {backoff_until.date()}"
                )
        else:
            # Reset on successful run
            self.state["consecutive_high_failures"] = 0
            self.state["is_open"] = False
        
        self._save_state()
        
        logger.info(
            f"Status check results for {self.domain}: "
            f"{total} checked, {failures} failed ({failure_rate:.0%})"
        )


def _stealth_head_fallback(url: str) -> Optional[Dict]:
    """
    2.9 Try to check URL using StealthFetcher when normal HEAD gets 403.
    
    Uses StealthFetcher's browser fingerprinting to bypass blocking.
    Returns a result dict compatible with check_url_head output.
    """
    if not STEALTH_AVAILABLE:
        return None
    
    try:
        fetcher = StealthFetcher()
        probe_result: ProbeResult = fetcher.fetch(url)
        
        if probe_result.success:
            result = {
                'url': url,
                'status_code': probe_result.status_code,
                'final_url': None,
                'is_redirect': False,
                'redirect_count': 0,
                'response_time_ms': None,
                'error': None,
                'checked_at': datetime.now(timezone.utc).isoformat(),
                'user_agent_used': f'stealth:{probe_result.strategy}',
                'h_etag': None,
                'h_last_modified': None,
                'h_content_length': None,
                'h_content_type': None,
                'h_cache_control': None,
                'h_age': None,
                'h_vary': None,
                'h_x_robots_tag': None,
                'h_link': None,
                'h_x_cache': None,
                'h_cf_cache_status': None,
                'headers_json': json.dumps(probe_result.headers) if probe_result.headers else None,
                'inferred_crawlable': True,
                'inferred_indexable_from_head': True,  # Assume true if we got through
                'h_canonical_url': None,
            }
            
            # Extract headers if available
            if probe_result.headers:
                h = probe_result.headers
                result['h_etag'] = h.get('ETag') or h.get('etag')
                result['h_last_modified'] = h.get('Last-Modified') or h.get('last-modified')
                result['h_content_length'] = h.get('Content-Length') or h.get('content-length')
                result['h_content_type'] = h.get('Content-Type') or h.get('content-type')
                result['h_x_robots_tag'] = h.get('X-Robots-Tag') or h.get('x-robots-tag')
            
            return result
        else:
            logger.warning(
                f"Stealth fallback failed for {url}: "
                f"status={probe_result.status_code}, error={probe_result.error}"
            )
            return None
            
    except Exception as e:
        logger.error(f"Stealth fallback error for {url}: {e}")
        return None


def check_url_head(
    url: str,
    user_agent: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT
) -> Dict:
    """
    3.0 Check URL status using HEAD request only.
    
    Captures full response headers for SEO intelligence:
    - X-Robots-Tag: Server-level indexing directives
    - ETag: Content fingerprint (changes = content updated)
    - Last-Modified: Server's modification timestamp
    - Content-Length: Page size (big changes = significant edits)
    - Cache-Control: Caching strategy
    - Age: Seconds since cached
    - Vary: Response variation signals
    - Link: Canonical URLs, hreflang alternates
    
    User agent selection:
    - Bankrate URLs → Bankratebot (our own property)
    - Competitor URLs → Random bot from rotation
    
    Args:
        url: URL to check
        user_agent: Override user agent (if None, auto-selects based on URL domain)
        timeout: Request timeout in seconds
    
    Returns dict with flattened key headers + full headers as JSON.
    """
    # 3.0.1 Select user agent based on URL domain (Bankrate vs competitor)
    if user_agent:
        selected_ua = user_agent
    else:
        selected_ua = get_user_agent_for_url(url)
    
    result = {
        'url': url,
        'status_code': None,
        'final_url': None,
        'is_redirect': False,
        'redirect_count': 0,
        'response_time_ms': None,
        'error': None,
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'user_agent_used': selected_ua,  # Track which UA was used
        
        # 3.1 Flattened key SEO headers (easy to query/filter)
        'h_etag': None,
        'h_last_modified': None,
        'h_content_length': None,
        'h_content_type': None,
        'h_cache_control': None,
        'h_age': None,
        'h_vary': None,
        'h_x_robots_tag': None,
        'h_link': None,
        'h_x_cache': None,
        'h_cf_cache_status': None,
        
        # 3.2 Full headers as JSON (for future analysis)
        'headers_json': None,
        
        # 3.3 Inferred status (from HEAD request)
        'inferred_crawlable': None,
        'inferred_indexable_from_head': None,
        'h_canonical_url': None,
    }
    
    headers = {'User-Agent': selected_ua}
    
    try:
        start = datetime.now()
        
        # HEAD request, follow redirects
        response = requests.head(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True
        )
        
        elapsed = (datetime.now() - start).total_seconds() * 1000
        
        # 3.3 Basic response info
        result['status_code'] = response.status_code
        result['final_url'] = response.url if response.url != url else None
        result['is_redirect'] = response.url != url
        result['redirect_count'] = len(response.history)
        result['response_time_ms'] = round(elapsed)
        
        # 3.3.1 Try stealth fallback on 403 Forbidden
        if response.status_code == 403 and STEALTH_AVAILABLE:
            stealth_result = _stealth_head_fallback(url)
            if stealth_result and stealth_result.get('status_code') == 200:
                logger.info(f"Stealth fallback succeeded for {url}")
                return stealth_result
        
        # 3.4 Extract key headers (flattened)
        resp_headers = response.headers
        result['h_etag'] = resp_headers.get('ETag')
        result['h_last_modified'] = resp_headers.get('Last-Modified')
        result['h_content_length'] = resp_headers.get('Content-Length')
        result['h_content_type'] = resp_headers.get('Content-Type')
        result['h_cache_control'] = resp_headers.get('Cache-Control')
        result['h_age'] = resp_headers.get('Age')
        result['h_vary'] = resp_headers.get('Vary')
        result['h_x_robots_tag'] = resp_headers.get('X-Robots-Tag')
        result['h_link'] = resp_headers.get('Link')
        
        # CDN cache headers (Cloudflare, Fastly, Akamai, etc.)
        result['h_x_cache'] = resp_headers.get('X-Cache')
        result['h_cf_cache_status'] = resp_headers.get('CF-Cache-Status')
        
        # 3.5 Store all headers as JSON for future flexibility
        try:
            result['headers_json'] = json.dumps(dict(resp_headers))
        except Exception:
            result['headers_json'] = None
        
        # 3.6 Inferred status fields (what we can determine from HEAD)
        # These help align with the Technical SEO Framework funnel
        x_robots = (result['h_x_robots_tag'] or '').lower()
        
        # Inferred crawlable: 200 status = server allows crawling
        result['inferred_crawlable'] = response.status_code == 200
        
        # Inferred indexable from HEAD: Check X-Robots-Tag for noindex
        # Note: This is incomplete - need GET request to check meta robots and canonical
        result['inferred_indexable_from_head'] = (
            response.status_code == 200 
            and 'noindex' not in x_robots 
            and 'none' not in x_robots
        )
        
        # Parse canonical from Link header if present
        link_header = result['h_link'] or ''
        canonical_from_header = None
        if 'rel="canonical"' in link_header or "rel='canonical'" in link_header:
            # Extract URL from Link header
            import re
            match = re.search(r'<([^>]+)>;\s*rel=["\']?canonical', link_header)
            if match:
                canonical_from_header = match.group(1)
        result['h_canonical_url'] = canonical_from_header
        
    except requests.exceptions.Timeout:
        result['error'] = 'timeout'
        result['status_code'] = 0
        result['inferred_crawlable'] = False
        result['inferred_indexable_from_head'] = False
    except requests.exceptions.ConnectionError as e:
        result['error'] = 'connection_error'
        result['status_code'] = 0
        result['inferred_crawlable'] = False
        result['inferred_indexable_from_head'] = False
    except requests.exceptions.TooManyRedirects:
        result['error'] = 'too_many_redirects'
        result['status_code'] = 0
        result['inferred_crawlable'] = False
        result['inferred_indexable_from_head'] = False
    except Exception as e:
        result['error'] = str(e)[:100]
        result['status_code'] = 0
        result['inferred_crawlable'] = False
        result['inferred_indexable_from_head'] = False
    
    return result


def check_url_content(
    url: str,
    user_agent: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT
) -> Dict:
    """
    3.5 Chain 3: GET request to extract SEO metadata from content.
    
    Only run AFTER HEAD check confirms URL is crawlable (200 status).
    
    Extracts:
    - <title> tag
    - <meta name="robots"> content
    - <link rel="canonical"> href
    - <meta name="description"> content
    - Open Graph tags (og:title, og:description)
    - Schema.org JSON-LD (type only, not full content)
    - Word count (approximate)
    
    Does NOT store full HTML content - just metadata for SEO analysis.
    
    Returns dict with content metadata + final inferred_indexable.
    """
    from bs4 import BeautifulSoup
    import re
    
    if user_agent:
        selected_ua = user_agent
    else:
        selected_ua = get_user_agent_for_url(url)
    
    result = {
        'url': url,
        'content_checked_at': datetime.now(timezone.utc).isoformat(),
        'content_status_code': None,
        'content_error': None,
        
        # SEO Metadata (flattened)
        'c_title': None,
        'c_title_length': None,
        'c_meta_description': None,
        'c_meta_description_length': None,
        'c_meta_robots': None,
        'c_canonical': None,
        'c_canonical_is_self': None,
        'c_og_title': None,
        'c_og_description': None,
        'c_h1': None,
        'c_h1_count': 0,
        'c_word_count': None,
        'c_schema_types': None,  # Comma-separated list of @type values
        
        # Final inferred status (combines HEAD + GET)
        'inferred_indexable': None,
        
        # Full metadata as JSON (for deep analysis)
        'content_meta_json': None,
    }
    
    headers = {
        'User-Agent': selected_ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
    }
    
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True
        )
        
        result['content_status_code'] = response.status_code
        
        if response.status_code != 200:
            result['content_error'] = f'status_{response.status_code}'
            result['inferred_indexable'] = False
            return result
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            result['c_title'] = title_tag.string.strip()[:500]
            result['c_title_length'] = len(result['c_title'])
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)})
        if meta_desc and meta_desc.get('content'):
            result['c_meta_description'] = meta_desc['content'][:500]
            result['c_meta_description_length'] = len(result['c_meta_description'])
        
        # Meta robots
        meta_robots = soup.find('meta', attrs={'name': re.compile(r'^robots$', re.I)})
        if meta_robots and meta_robots.get('content'):
            result['c_meta_robots'] = meta_robots['content'].lower()
        
        # Canonical
        canonical = soup.find('link', attrs={'rel': 'canonical'})
        if canonical and canonical.get('href'):
            result['c_canonical'] = canonical['href']
            # Check if self-referencing
            result['c_canonical_is_self'] = (
                canonical['href'].rstrip('/') == url.rstrip('/') or
                canonical['href'].rstrip('/') == response.url.rstrip('/')
            )
        
        # Open Graph
        og_title = soup.find('meta', attrs={'property': 'og:title'})
        if og_title and og_title.get('content'):
            result['c_og_title'] = og_title['content'][:200]
        
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc and og_desc.get('content'):
            result['c_og_description'] = og_desc['content'][:500]
        
        # H1 tags
        h1_tags = soup.find_all('h1')
        result['c_h1_count'] = len(h1_tags)
        if h1_tags:
            result['c_h1'] = h1_tags[0].get_text(strip=True)[:200]
        
        # Word count (approximate - text content only)
        text = soup.get_text(separator=' ', strip=True)
        words = text.split()
        result['c_word_count'] = len(words)
        
        # Schema.org JSON-LD types
        schema_types = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict) and '@type' in data:
                    schema_types.append(data['@type'])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and '@type' in item:
                            schema_types.append(item['@type'])
            except:
                pass
        if schema_types:
            result['c_schema_types'] = ','.join(schema_types[:10])  # Limit to 10
        
        # Final inferred indexable (complete picture)
        meta_robots_content = result['c_meta_robots'] or ''
        result['inferred_indexable'] = (
            result['content_status_code'] == 200
            and 'noindex' not in meta_robots_content
            and 'none' not in meta_robots_content
            and (result['c_canonical_is_self'] is True or result['c_canonical'] is None)
        )
        
        # Store full metadata as JSON
        try:
            meta_dict = {
                'title': result['c_title'],
                'meta_description': result['c_meta_description'],
                'meta_robots': result['c_meta_robots'],
                'canonical': result['c_canonical'],
                'canonical_is_self': result['c_canonical_is_self'],
                'og_title': result['c_og_title'],
                'og_description': result['c_og_description'],
                'h1': result['c_h1'],
                'h1_count': result['c_h1_count'],
                'word_count': result['c_word_count'],
                'schema_types': schema_types,
            }
            result['content_meta_json'] = json.dumps(meta_dict)
        except:
            pass
        
    except requests.exceptions.Timeout:
        result['content_error'] = 'timeout'
        result['content_status_code'] = 0
        result['inferred_indexable'] = False
    except requests.exceptions.ConnectionError:
        result['content_error'] = 'connection_error'
        result['content_status_code'] = 0
        result['inferred_indexable'] = False
    except Exception as e:
        result['content_error'] = str(e)[:100]
        result['content_status_code'] = 0
        result['inferred_indexable'] = False
    
    return result


def get_urls_to_check(
    domain: str,
    data_dir: str,
    check_new: bool,
    check_updated: bool,
    check_removed: bool,
    max_per_run: int,
    days_back: int = 7
) -> List[Dict]:
    """
    4.0 Get URLs to check from recent change logs.
    
    Prioritizes:
    1. Removed URLs (most important to verify)
    2. New URLs (verify they're live)
    3. Updated URLs (verify changes)
    """
    domain_dir = os.path.join(data_dir, domain)
    
    # Find recent change log files
    change_files = glob(os.path.join(domain_dir, f"{domain}_changes_*.csv"))
    
    if not change_files:
        logger.info(f"No change logs found for {domain}")
        return []
    
    # Load and combine recent changes
    all_changes = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
    
    for file_path in sorted(change_files, reverse=True):
        try:
            df = pd.read_csv(file_path)
            if 'detected_at' in df.columns:
                # Use utc=True to handle mixed timezone formats consistently
                df['detected_at'] = pd.to_datetime(df['detected_at'], errors='coerce', utc=True)
                df = df[df['detected_at'] >= cutoff_date]
            all_changes.append(df)
        except Exception as e:
            logger.warning(f"Could not read {file_path}: {e}")
    
    if not all_changes:
        return []
    
    combined = pd.concat(all_changes, ignore_index=True)
    
    # Filter by change types we want to check
    change_types = []
    if check_removed:
        change_types.append('removed')
    if check_new:
        change_types.append('discovered')
    if check_updated:
        change_types.append('modified')
    
    filtered = combined[combined['change_type'].isin(change_types)]
    
    # Deduplicate by URL, keep most recent
    if not filtered.empty and 'detected_at' in filtered.columns:
        filtered = filtered.sort_values('detected_at', ascending=False)
        filtered = filtered.drop_duplicates(subset=['loc'], keep='first')
    
    # Prioritize: removed > discovered > modified
    priority_map = {'removed': 0, 'discovered': 1, 'modified': 2}
    filtered['priority'] = filtered['change_type'].map(priority_map)
    filtered = filtered.sort_values('priority')
    
    # Limit
    filtered = filtered.head(max_per_run)
    
    logger.info(
        f"Found {len(filtered)} URLs to check for {domain}: "
        f"{len(filtered[filtered['change_type']=='removed'])} removed, "
        f"{len(filtered[filtered['change_type']=='discovered'])} discovered, "
        f"{len(filtered[filtered['change_type']=='modified'])} modified"
    )
    
    return filtered.to_dict('records')


def check_urls_for_domain(
    domain: str,
    config: Dict,
    data_dir: str = "data",
    force: bool = False
) -> Optional[pd.DataFrame]:
    """
    5.0 Run status checks for a domain.
    
    Includes:
    - Random shuffle of URL order (avoid sequential patterns)
    - Random delay between requests (1-3s, politeness not stealth)
    - Full header capture for SEO intelligence
    """
    domain_config = get_domain_status_config(config, domain)
    
    # Check if enabled
    if not domain_config.get("enabled", True) and not force:
        logger.info(f"Status checking disabled for {domain}")
        return None
    
    # Check circuit breaker
    circuit = CircuitBreaker(domain, data_dir)
    if not circuit.should_check(
        domain_config["failure_threshold"],
        domain_config["backoff_days"]
    ) and not force:
        return None
    
    # Get URLs to check
    urls = get_urls_to_check(
        domain=domain,
        data_dir=data_dir,
        check_new=domain_config.get("check_new", True),
        check_updated=domain_config.get("check_updated", True),
        check_removed=domain_config.get("check_removed", True),
        max_per_run=domain_config.get("max_per_run", 100),
    )
    
    if not urls:
        logger.info(f"No URLs to check for {domain}")
        return None
    
    # 5.1 Shuffle URL order to avoid sequential patterns
    random.shuffle(urls)
    logger.info(f"Checking {len(urls)} URLs for {domain} (shuffled order)")
    
    # Run checks
    results = []
    failures = 0
    
    # 5.2 Get delay settings (default 1.5-4 seconds between requests)
    base_delay = domain_config.get("base_delay", 2.5)
    delay_jitter = domain_config.get("delay_jitter", 1.5)
    
    for i, url_record in enumerate(urls):
        url = url_record.get('loc')
        if not url:
            continue
        
        # 5.3 Human-like delay pattern between requests
        if i > 0:
            # Gaussian distribution centered on base_delay for more natural timing
            delay = max(0.5, random.gauss(base_delay, delay_jitter))
            
            # Occasional longer pause (10% chance) - simulates human distraction
            if random.random() < 0.10:
                delay += random.uniform(3, 8)
            
            # Brief burst pattern (5% chance of quick follow-up)
            if random.random() < 0.05:
                delay = random.uniform(0.3, 0.8)
            
            time.sleep(delay)
        
        if (i + 1) % 20 == 0:
            logger.info(f"Progress: {i + 1}/{len(urls)}")
        
        result = check_url_head(
            url,
            user_agent=None,  # Auto-select based on URL domain
            timeout=domain_config.get("timeout", DEFAULT_TIMEOUT)
        )
        
        # Add context from change log
        result['domain'] = domain
        result['change_type'] = url_record.get('change_type')
        result['section'] = url_record.get('section')
        
        # 5.4 Classify result by status code
        if result['status_code'] == 0:
            result['fate'] = 'error'
            failures += 1
        elif result['status_code'] == 200:
            result['fate'] = 'live'
        elif result['status_code'] == 404:
            result['fate'] = 'not_found'
        elif result['status_code'] == 410:
            result['fate'] = 'gone'
        elif 300 <= result['status_code'] < 400:
            result['fate'] = 'redirect'
        elif result['status_code'] == 403:
            result['fate'] = 'forbidden'
            failures += 1  # Count as failure (we're being blocked)
        elif result['status_code'] == 429:
            result['fate'] = 'rate_limited'
            failures += 1
        elif result['status_code'] >= 500:
            result['fate'] = 'server_error'
        else:
            result['fate'] = f'other_{result["status_code"]}'
        
        # 5.5 Detect X-Robots-Tag signals
        x_robots = result.get('h_x_robots_tag', '')
        if x_robots:
            x_robots_lower = x_robots.lower()
            result['has_noindex'] = 'noindex' in x_robots_lower
            result['has_nofollow'] = 'nofollow' in x_robots_lower
        else:
            result['has_noindex'] = False
            result['has_nofollow'] = False
        
        results.append(result)
    
    # Record in circuit breaker
    circuit.record_results(
        total=len(results),
        failures=failures,
        failure_threshold=domain_config["failure_threshold"],
        backoff_days=domain_config["backoff_days"]
    )
    
    return pd.DataFrame(results) if results else None


def save_daily_history(df: pd.DataFrame, domain: str, data_dir: str = "data") -> str:
    """
    6.0 Save status check results to daily history file.
    
    Saves two files:
    1. Main history CSV with flattened key headers
    2. Full headers JSON file for detailed analysis
    """
    if df is None or df.empty:
        return None
    
    domain_dir = os.path.join(data_dir, domain)
    os.makedirs(domain_dir, exist_ok=True)
    
    # Daily file paths
    date_str = datetime.now().strftime("%Y-%m-%d")
    history_path = os.path.join(domain_dir, f"{domain}_status_history_{date_str}.csv")
    headers_path = os.path.join(domain_dir, f"{domain}_headers_{date_str}.jsonl")
    
    # 6.1 Column order for main CSV (flattened headers included)
    columns = [
        # Core fields
        'domain', 'url', 'change_type', 'fate', 'status_code',
        'is_redirect', 'final_url', 'redirect_count',
        'response_time_ms', 'section', 'checked_at', 'error',
        # X-Robots signals
        'has_noindex', 'has_nofollow',
        # Key SEO headers (flattened)
        'h_etag', 'h_last_modified', 'h_content_length', 'h_content_type',
        'h_cache_control', 'h_age', 'h_vary', 'h_x_robots_tag', 'h_link',
        'h_x_cache', 'h_cf_cache_status',
    ]
    available = [c for c in columns if c in df.columns]
    output_df = df[available]
    
    # 6.2 Save main CSV (append mode)
    if os.path.exists(history_path):
        output_df.to_csv(history_path, mode='a', header=False, index=False)
        logger.info(f"Appended {len(df)} results to {history_path}")
    else:
        output_df.to_csv(history_path, mode='w', header=True, index=False)
        logger.info(f"Created {history_path} with {len(df)} results")
    
    # 6.3 Save full headers as JSONL (one JSON object per line)
    if 'headers_json' in df.columns:
        try:
            with open(headers_path, 'a') as f:
                for _, row in df.iterrows():
                    if row.get('headers_json'):
                        record = {
                            'url': row.get('url'),
                            'checked_at': row.get('checked_at'),
                            'status_code': row.get('status_code'),
                            'headers': json.loads(row['headers_json']) if row['headers_json'] else {}
                        }
                        f.write(json.dumps(record) + '\n')
            logger.info(f"Saved full headers to {headers_path}")
        except Exception as e:
            logger.warning(f"Could not save headers JSONL: {e}")
    
    return history_path


def generate_redirect_map(df: pd.DataFrame, domain: str, data_dir: str = "data") -> Optional[str]:
    """
    7.0 Generate old -> new URL mapping from redirects.
    """
    if df is None or df.empty:
        return None
    
    redirects = df[(df['fate'] == 'redirect') & (df['final_url'].notna())].copy()
    
    if redirects.empty:
        return None
    
    mapping = redirects[['url', 'final_url', 'status_code', 'change_type']].copy()
    mapping.columns = ['old_url', 'new_url', 'redirect_code', 'original_change_type']
    
    # Check if redirect stays on same domain
    def same_domain(old, new):
        try:
            return urlparse(old).netloc == urlparse(new).netloc
        except:
            return False
    
    mapping['same_domain'] = mapping.apply(
        lambda r: same_domain(r['old_url'], r['new_url']), axis=1
    )
    
    domain_dir = os.path.join(data_dir, domain)
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(domain_dir, f"{domain}_redirect_map_{date_str}.csv")
    
    mapping.to_csv(output_path, index=False)
    logger.info(f"Saved redirect map: {len(mapping)} mappings to {output_path}")
    
    return output_path


def print_summary(df: pd.DataFrame, domain: str):
    """
    8.0 Print summary of status check results.
    """
    if df is None or df.empty:
        return
    
    print(f"\n{'='*50}")
    print(f"Status Check Summary: {domain}")
    print(f"{'='*50}")
    
    # By fate
    print("\nBy Fate:")
    fate_counts = df['fate'].value_counts()
    for fate, count in fate_counts.items():
        pct = count / len(df) * 100
        print(f"  {fate}: {count} ({pct:.1f}%)")
    
    # By change type
    if 'change_type' in df.columns:
        print("\nBy Change Type:")
        for ct in ['removed', 'discovered', 'modified']:
            subset = df[df['change_type'] == ct]
            if len(subset) > 0:
                print(f"  {ct}: {len(subset)} URLs")
                fate_dist = subset['fate'].value_counts().head(3)
                for fate, count in fate_dist.items():
                    print(f"    → {fate}: {count}")
    
    # Response times (excluding errors)
    valid = df[df['response_time_ms'].notna() & (df['response_time_ms'] > 0)]
    if len(valid) > 0:
        print(f"\nResponse Times:")
        print(f"  Median: {valid['response_time_ms'].median():.0f}ms")
        print(f"  95th percentile: {valid['response_time_ms'].quantile(0.95):.0f}ms")
    
    print(f"{'='*50}\n")


def process_domain_status(
    domain: str,
    config: Dict[str, Any],
    data_dir: str,
    force: bool,
    limit: Optional[int] = None
) -> Tuple[str, Dict[str, Any]]:
    """
    9.0 Process status checks for a single domain (designed for concurrent execution).
    
    Args:
        domain: Domain to check
        config: Global configuration dict
        data_dir: Data directory path
        force: Force run even if disabled
        limit: Override max URLs per domain
        
    Returns:
        Tuple of (domain, result_dict) for aggregation
    """
    try:
        logger.info(f"\n--- Checking {domain} ---")
        
        # Override limit if specified
        if limit:
            domain_config = get_domain_status_config(config, domain)
            domain_config["max_per_run"] = limit
            # Note: We don't modify shared config here to avoid race conditions
        
        results_df = check_urls_for_domain(
            domain=domain,
            config=config,
            data_dir=data_dir,
            force=force
        )
        
        if results_df is not None and not results_df.empty:
            # Save history
            save_daily_history(results_df, domain, data_dir)
            
            # Generate redirect map
            generate_redirect_map(results_df, domain, data_dir)
            
            # Print summary
            print_summary(results_df, domain)
            
            return (domain, {"status": "success", "urls_checked": len(results_df)})
        else:
            return (domain, {"status": "skipped", "message": "No URLs to check or disabled"})
            
    except Exception as e:
        logger.error(f"FAILED processing status checks for {domain}: {type(e).__name__}: {e}")
        logger.exception("Full traceback:")
        return (domain, {"status": "error", "message": str(e)})


def main():
    """
    9.1 CLI entry point with concurrent domain processing.
    """
    parser = argparse.ArgumentParser(
        description="Check HTTP status of URLs from sitemap changes"
    )
    parser.add_argument(
        "--domain", "-d",
        default=None,
        help="Specific domain to check (default: all enabled domains)"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Data directory (default: data)"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force run even if disabled or circuit is open"
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Override max URLs per domain"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Starting URL Status Checker")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)
    
    config = load_config()
    
    # Determine which domains to check
    if args.domain:
        domains = [args.domain]
    else:
        domains = [t.get("domain") for t in config.get("targets", []) if t.get("domain")]
    
    if not domains:
        logger.error("No domains configured")
        return
    
    # Get concurrency setting (default 4, or 1 if single domain specified)
    max_concurrent = 1 if args.domain else config.get("max_concurrent_domains", 4)
    logger.info(f"Processing {len(domains)} domains with {max_concurrent} concurrent workers")
    
    # Process domains concurrently
    domain_results = {}
    
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {
            executor.submit(
                process_domain_status,
                domain,
                config,
                args.data_dir,
                args.force,
                args.limit
            ): domain
            for domain in domains
        }
        
        for future in as_completed(futures):
            domain = futures[future]
            try:
                domain_name, result = future.result()
                domain_results[domain_name] = result
                logger.info(f"Completed {domain_name}: {result.get('status')}")
            except Exception as e:
                logger.error(f"Error retrieving result for {domain}: {e}")
                domain_results[domain] = {"status": "error", "message": str(e)}
    
    # Summary
    logger.info("=" * 60)
    logger.info("URL Status Checker complete")
    logger.info(f"Results: {domain_results}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

