"""
1.0 Main Orchestrator Module
Coordinates the sitemap monitoring pipeline with scheduling and change detection.

Key features:
- Recursive sitemap index traversal
- Tags each URL with its source sitemap
- Configurable scheduling (daily, weekly, monthly, custom intervals)
- Random scheduling for non-priority domains
- User-agent rotation
"""

import logging
import os
import random
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# Project-specific imports
from src.config import load_config, CONFIG_FILE_PATH
from src.sitemap_fetcher import SitemapFetcher
from src.sitemap_parser import SitemapParser
from src.data_processor import DataProcessor
from src.robots_checker import RobotsChecker

# 1.1 Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 1.2 User agent configuration
# Bankratebot for our own properties, random bots for competitors
BANKRATE_USER_AGENT = "Mozilla/5.0 (compatible; Bankratebot/1.0; +https://www.bankrate.com)"
BANKRATE_DOMAINS = ["bankrate.com", "www.bankrate.com"]

# =============================================================================
# BOT USER AGENTS FOR COMPETITOR CRAWLING
# All documented bots that respect robots.txt
# We filter by robots.txt, then pick randomly from what's allowed
# Source: https://dejan.ai/blog/ai-bots/ (Nov 2025)
# =============================================================================

COMPETITOR_BOT_USER_AGENTS = [
    # Our own bot
    "Mozilla/5.0 (compatible; SitemapMonitor/1.0; +https://github.com/ldriggers/xml-sitemap-monitor)",
    # OpenAI bots
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.1; +https://openai.com/gptbot)",
    # Other search/AI bots
    "Mozilla/5.0 (compatible; DuckDuckBot/1.1; +http://duckduckgo.com/duckduckbot)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; MistralAI-User/1.0; +https://docs.mistral.ai/robots)",
    # Anthropic
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)",
    # Perplexity
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)",
]

# Browser fallback when no bots are allowed
BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# Module-level robots checker (caches robots.txt)
_robots_checker = None

def get_robots_checker() -> RobotsChecker:
    """Get or create the robots checker singleton."""
    global _robots_checker
    if _robots_checker is None:
        _robots_checker = RobotsChecker()
    return _robots_checker


def get_user_agent(config: Dict[str, Any], domain: str) -> str:
    """
    2.0 Get the appropriate user agent for a domain.
    
    Logic:
    - Bankrate domains → Bankratebot (our own properties)
    - Competitor domains → Random bot from allowed bots (respects robots.txt)
    - If all bots blocked → Use browser UA
    - Config override → Use if explicitly set
    
    Args:
        config: Configuration dictionary
        domain: The domain being processed
        
    Returns:
        User agent string
    """
    # Check for domain-specific user agent override in config
    for target in config.get("targets", []):
        if target.get("domain") == domain:
            if target.get("user_agent"):
                return target["user_agent"]
    
    # Use Bankratebot for Bankrate properties
    if any(bd in domain for bd in BANKRATE_DOMAINS):
        logger.info(f"Using Bankratebot for own property: {domain}")
        return BANKRATE_USER_AGENT
    
    # For competitor domains: filter by robots.txt, pick randomly from allowed
    robots_checker = get_robots_checker()
    
    # Get domain with www prefix if needed
    check_domain = domain if domain.startswith("www.") else f"www.{domain}"
    
    # Filter to only allowed bots, then pick randomly
    allowed_bots = robots_checker.filter_allowed_bots(check_domain, COMPETITOR_BOT_USER_AGENTS)
    
    if allowed_bots:
        selected = random.choice(allowed_bots)
        logger.info(f"Using bot for {domain}: {selected[:50]}...")
        return selected
    
    # All bots blocked - use browser UA (will trigger stealth if needed)
    logger.warning(f"All bots blocked by robots.txt for {domain}, using browser UA")
    return BROWSER_USER_AGENT


def calculate_startup_jitter(domain: str, random_config: Dict[str, Any]) -> int:
    """
    2.5 Calculate minimal startup jitter to avoid exact-second predictability.
    
    NOTE: Primary stealth is now handled at the request level via
    SitemapFetcher's humanized delays. This startup jitter is just
    to add slight variance to when the process begins.
    
    Args:
        domain: The domain being processed (used for deterministic seed)
        random_config: Randomization settings from config
        
    Returns:
        Number of seconds (not minutes) to wait before starting
    """
    # Seed for deterministic but varied behavior per day + domain
    seed_str = f"{datetime.now(timezone.utc).date().isoformat()}::{domain}::startup"
    rng = random.Random(seed_str)
    
    # Small jitter: 0-120 seconds (2 minutes max)
    # Enough variance without wasting CI time
    max_jitter_seconds = int(random_config.get("max_startup_jitter_seconds", 120) or 120)
    jitter_seconds = rng.randint(0, max_jitter_seconds)
    
    logger.debug(f"Startup jitter for {domain}: {jitter_seconds}s")
    
    return jitter_seconds


def should_run_domain(target: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """
    3.0 Determine if a domain should be processed on this run.
    
    Default behavior: ALL domains run every day.
    
    Optional overrides (if explicitly set in config):
    - enabled: False to completely disable a domain
    - interval_days: Number of days between runs (only if you need less frequent)
    
    Args:
        target: Target configuration dictionary
        config: Global configuration dictionary
        
    Returns:
        True if domain should be processed, False to skip
    """
    domain = target.get("domain", "")
    
    # 3.1 Check if explicitly disabled
    if target.get("enabled") is False:
        logger.info(f"Domain {domain}: disabled in config, skipping")
        return False
    
    # 3.2 Check interval_days if explicitly set > 1 (rare case)
    interval_days = int(target.get("interval_days", 1))
    
    if interval_days > 1:
        today = datetime.now(timezone.utc).date()
        # Use deterministic seed based on domain so it's consistent
        seed_str = f"{domain}"
        rng = random.Random(seed_str)
        
        # Pick a start day for this domain's schedule
        start_day = rng.randint(0, interval_days - 1)
        days_since_epoch = (today - datetime(1970, 1, 1).date()).days
        
        if (days_since_epoch - start_day) % interval_days != 0:
            logger.info(f"Domain {domain}: interval_days={interval_days}, not scheduled today")
            return False
    
    logger.info(f"Domain {domain}: scheduled to run")
    return True


def process_single_sitemap_url(
    sitemap_url: str,
    fetcher: SitemapFetcher,
    parser: SitemapParser,
    processed_sitemap_urls: set,
    domain: str,
    sitemap_file_records: Optional[List[Dict[str, Any]]] = None,
) -> list:
    """
    4.0 Fetch and parse a single sitemap URL (index or urlset).
    
    Recursively processes sitemap indexes and collects all page URLs.
    Tags each URL with its source sitemap.
    
    Args:
        sitemap_url: URL of the sitemap to process
        fetcher: SitemapFetcher instance
        parser: SitemapParser instance
        processed_sitemap_urls: Set of already-processed sitemap URLs (to avoid duplicates)
        domain: The domain being processed
        sitemap_file_records: Optional list to collect sitemap file metadata
        
    Returns:
        List of page URL dictionaries with sitemap_source_url field
    """
    if sitemap_url in processed_sitemap_urls:
        logger.info(f"Sitemap {sitemap_url} already processed. Skipping.")
        return []

    logger.info(f"Processing sitemap: {sitemap_url}")
    processed_sitemap_urls.add(sitemap_url)
    
    xml_content = fetcher.fetch_sitemap_xml(sitemap_url)

    if not xml_content:
        logger.warning(f"Failed to fetch XML content for {sitemap_url}. Skipping.")
        return []

    # 4.1 Record sitemap file metadata (for XML tracking)
    if sitemap_file_records is not None:
        try:
            content_hash = hashlib.sha256(xml_content.encode("utf-8")).hexdigest()
            sitemap_record = {
                "sitemap_url": sitemap_url,
                "domain": domain,
                "sitemap_type": None,  # Will be filled after parsing
                "url_count": 0,        # Will be filled after parsing
                "content_hash": content_hash,
                "content_length": len(xml_content),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logger.warning(f"Could not hash sitemap {sitemap_url}: {e}")
            sitemap_record = None
    else:
        sitemap_record = None

    parsed_data = parser.parse_sitemap(xml_content, sitemap_url=sitemap_url)

    # 4.2 Complete the sitemap record now that we know the type and count
    if sitemap_record is not None:
        sitemap_record["sitemap_type"] = parsed_data.get("type")
        sitemap_record["url_count"] = parsed_data.get("url_count", 0)
        sitemap_file_records.append(sitemap_record)

    all_page_urls_from_this_branch = []

    if parsed_data["type"] == "sitemapindex":
        sub_sitemaps = parsed_data.get("urls", []) or []
        logger.info(f"Sitemap index {sitemap_url} contains {len(sub_sitemaps)} sub-sitemaps.")
        
        for sub_sitemap in sub_sitemaps:
            # Handle both old format (string) and new format (dict with loc/lastmod)
            if isinstance(sub_sitemap, dict):
                sub_url = sub_sitemap.get('loc')
                sub_lastmod = sub_sitemap.get('lastmod')
                
                # Record sub-sitemap metadata
                if sitemap_file_records is not None and sub_lastmod:
                    # Update the parent record or create a pre-record for this sub-sitemap
                    logger.debug(f"Sub-sitemap {sub_url} has lastmod: {sub_lastmod}")
            else:
                sub_url = sub_sitemap
            
            if sub_url:
                all_page_urls_from_this_branch.extend(
                    process_single_sitemap_url(
                        sitemap_url=sub_url,
                        fetcher=fetcher,
                        parser=parser,
                        processed_sitemap_urls=processed_sitemap_urls,
                        domain=domain,
                        sitemap_file_records=sitemap_file_records,
                    )
                )
            
    elif parsed_data["type"] == "urlset":
        page_urls_in_set = parsed_data.get("urls", []) or []
        logger.info(f"URL set {sitemap_url} contains {len(page_urls_in_set)} page URLs.")
        
        # 4.3 TAG EACH URL WITH ITS SOURCE SITEMAP
        for entry in page_urls_in_set:
            if isinstance(entry, dict):
                entry["sitemap_source_url"] = sitemap_url
        
        all_page_urls_from_this_branch.extend(page_urls_in_set)
        
    elif parsed_data["type"] == "error":
        logger.error(f"Error parsing sitemap {sitemap_url}: {parsed_data.get('error_message')}")
    else:
        logger.warning(f"Unknown sitemap type '{parsed_data['type']}' for {sitemap_url}.")

    return all_page_urls_from_this_branch


def process_domain(
    target: Dict[str, Any],
    config: Dict[str, Any],
    data_processor: DataProcessor,
    stealth_config: Dict[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    """
    4.5 Process a single domain (designed for concurrent execution).
    
    Args:
        target: Target configuration dictionary
        config: Global configuration dictionary
        data_processor: DataProcessor instance (thread-safe for different domains)
        stealth_config: Stealth/timing settings
        
    Returns:
        Tuple of (domain, result_dict) for aggregation
    """
    domain = target.get("domain")
    # Support both single sitemap_url and array of sitemap_urls
    sitemap_url = target.get("sitemap_url")
    sitemap_urls = target.get("sitemap_urls", [])
    
    # Normalize to list
    if sitemap_url and not sitemap_urls:
        sitemap_urls = [sitemap_url]
    
    try:
        # 4.5.1 Skip jitter for Bankrate (own property) and when disabled
        is_bankrate = any(bd in domain for bd in BANKRATE_DOMAINS)
        jitter_enabled = stealth_config.get("enabled", False) and not is_bankrate
        
        if jitter_enabled:
            jitter_seconds = calculate_startup_jitter(domain, stealth_config)
            if jitter_seconds > 0:
                logger.info(f"Startup jitter: {jitter_seconds}s for {domain}")
                time.sleep(jitter_seconds)

        logger.info(f"Processing domain: {domain}, sitemaps: {len(sitemap_urls)}")

        # 4.5.2 Create fetcher with appropriate user agent and target-specific settings
        user_agent = get_user_agent(config, domain)
        fetcher_config = {
            **config,
            "user_agent": user_agent,
            "timeout": target.get("fetch_timeout", target.get("timeout", config.get("timeout", 30))),
            "download_delay": target.get("download_delay", config.get("download_delay", 1.5)),
        }
        sitemap_fetcher = SitemapFetcher(config=fetcher_config)
        sitemap_parser = SitemapParser()

        # 4.5.3 Collect sitemap file metadata
        processed_sitemap_urls_for_domain = set()
        sitemap_file_records: List[Dict[str, Any]] = []

        # 4.5.4 Recursively fetch all page URLs from all sitemaps
        all_page_url_dicts = []
        for sm_url in sitemap_urls:
            logger.info(f"Fetching sitemap: {sm_url}")
            urls_from_sitemap = process_single_sitemap_url(
                sitemap_url=sm_url,
                fetcher=sitemap_fetcher,
                parser=sitemap_parser,
                processed_sitemap_urls=processed_sitemap_urls_for_domain,
                domain=domain,
                sitemap_file_records=sitemap_file_records,
            )
            all_page_url_dicts.extend(urls_from_sitemap)

        if not all_page_url_dicts:
            logger.warning(f"No page URLs found for {domain}. Skipping.")
            return (domain, {"status": "warning", "message": "No URLs found"})

        logger.info(f"Gathered {len(all_page_url_dicts)} page URLs for {domain}")
        logger.info(f"Processed {len(sitemap_file_records)} sitemap files for {domain}")

        # 4.5.5 Save sitemap file metadata
        if sitemap_file_records:
            data_processor.save_sitemap_metadata(domain, sitemap_file_records)

        # 4.5.6 Log sample for diagnostics
        if all_page_url_dicts:
            sample = all_page_url_dicts[0]
            logger.debug(f"Sample URL: {sample.get('loc')}, section: {sample.get('section')}")

        # 4.5.7 Process URLs and track changes
        urls_df = data_processor.process_sitemap_urls(domain, all_page_url_dicts)

        logger.info(f"Completed processing for domain: {domain}")
        return (domain, {"status": "success", "urls": len(all_page_url_dicts)})
        
    except Exception as e:
        # 4.5.8 Log error but don't crash - return error result
        logger.error(f"FAILED processing domain {domain}: {type(e).__name__}: {e}")
        logger.exception("Full traceback:")
        return (domain, {"status": "error", "message": str(e)})


def main():
    """
    5.0 Main function to orchestrate the sitemap processing pipeline.
    
    Flow:
    1. Load configuration
    2. For each target domain (if scheduled):
       a. Apply optional jitter delay
       b. Recursively fetch all sitemap URLs
       c. Tag each URL with its source sitemap
       d. Process and detect changes
       e. Save to per-domain folder structure
    """
    logger.info("=" * 60)
    logger.info("Starting sitemap processing pipeline")
    logger.info(f"Run timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # 5.1 Load configuration
    config = load_config()
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return

    # 5.2 Initialize components
    data_dir = config.get("data_directory", "output")
    os.makedirs(data_dir, exist_ok=True)

    data_processor = DataProcessor(data_dir=data_dir)
    
    # Get stealth/timing settings
    stealth_config = config.get("stealth", {})

    # 5.3 Filter targets to process
    targets_to_process = []
    for target in config.get("targets", []):
        domain = target.get("domain")
        # Support both single sitemap_url and array of sitemap_urls
        sitemap_url = target.get("sitemap_url")
        sitemap_urls = target.get("sitemap_urls", [])
        
        has_sitemaps = sitemap_url or sitemap_urls
        if not domain or not has_sitemaps:
            logger.warning(f"Skipping target with missing domain or sitemap_url(s): {target}")
            continue

        if not should_run_domain(target, config):
            continue
            
        targets_to_process.append(target)
    
    logger.info(f"Processing {len(targets_to_process)} domains")
    
    # 5.4 Process domains concurrently (configurable worker count)
    # Default: 4 workers for balance of speed vs resource usage
    # Can scale to 6-8 for 40+ domains
    max_workers = config.get("max_concurrent_domains", 4)
    domain_results = {}
    
    if len(targets_to_process) == 0:
        logger.warning("No domains to process")
    elif len(targets_to_process) == 1 or max_workers == 1:
        # Single domain or sequential mode - no threading overhead
        for target in targets_to_process:
            domain, result = process_domain(target, config, data_processor, stealth_config)
            domain_results[domain] = result
            logger.info("-" * 40)
    else:
        # Concurrent processing with ThreadPoolExecutor
        logger.info(f"Using {max_workers} concurrent workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all domain processing tasks
            future_to_domain = {
                executor.submit(
                    process_domain, target, config, data_processor, stealth_config
                ): target.get("domain")
                for target in targets_to_process
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_domain):
                domain = future_to_domain[future]
                try:
                    result_domain, result = future.result()
                    domain_results[result_domain] = result
                    logger.info(f"Finished {result_domain}: {result.get('status')}")
                except Exception as e:
                    logger.error(f"Unexpected error for {domain}: {e}")
                    domain_results[domain] = {"status": "error", "message": str(e)}
    
    # 5.5 Summary of domain results
    logger.info("=" * 60)
    logger.info("Domain Processing Summary:")
    for domain, result in domain_results.items():
        status = result.get("status", "unknown")
        if status == "success":
            logger.info(f"  [OK] {domain}: {result.get('urls', 0)} URLs processed")
        elif status == "warning":
            logger.warning(f"  [WARN] {domain}: {result.get('message', 'warning')}")
        else:
            logger.error(f"  [FAIL] {domain}: {result.get('message', 'failed')}")

    logger.info("=" * 60)
    logger.info("Sitemap processing pipeline completed")
    logger.info("=" * 60)


if __name__ == "__main__":
    # 6.0 Entry point - handle running from different directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    potential_config_path = os.path.join(project_root, CONFIG_FILE_PATH)

    if os.getcwd() == script_dir and os.path.exists(potential_config_path):
        logger.info(f"Running from src/, changing CWD to project root: {project_root}")
        os.chdir(project_root)
    elif not os.path.exists(CONFIG_FILE_PATH):
        if os.path.exists(potential_config_path):
            logger.info(f"Config not in CWD, found at project root. Changing CWD to: {project_root}")
            os.chdir(project_root)
        else:
            logger.warning(f"Config file not found in CWD or project root. load_config might fail.")

    main()
