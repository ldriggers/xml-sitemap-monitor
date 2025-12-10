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
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Project-specific imports
from src.config import load_config, CONFIG_FILE_PATH
from src.sitemap_fetcher import SitemapFetcher
from src.sitemap_parser import SitemapParser
from src.data_processor import DataProcessor

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

# Bot user agents for competitor crawling
# Mix of AI bots and traditional search engine bots
COMPETITOR_BOT_USER_AGENTS = [
    # OpenAI bots
    "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)",
    "Mozilla/5.0 (compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot)",
    "Mozilla/5.0 (compatible; ChatGPT-User/1.0; +https://openai.com/chatgpt-user)",
    # Anthropic/Claude bots
    "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://www.anthropic.com/claude)",
    "Mozilla/5.0 (compatible; Claude-User/1.0; +https://www.anthropic.com)",
    "Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +https://www.anthropic.com)",
    # Perplexity bot
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
]


def get_user_agent(config: Dict[str, Any], domain: str) -> str:
    """
    2.0 Get the appropriate user agent for a domain.
    
    Logic:
    - Bankrate domains → Bankratebot (our own properties)
    - Competitor domains → Random bot from rotation
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
    
    # Use random bot for competitor domains
    selected = random.choice(COMPETITOR_BOT_USER_AGENTS)
    logger.info(f"Using random bot for competitor {domain}: {selected[:50]}...")
    return selected


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
    data_dir = config.get("data_directory", "data")
    os.makedirs(data_dir, exist_ok=True)

    data_processor = DataProcessor(data_dir=data_dir)
    
    # Get stealth/timing settings
    stealth_config = config.get("stealth", {})

    # 5.3 Process each target domain (fault-tolerant: one failure doesn't stop others)
    domain_results = {}  # Track success/failure per domain
    
    for target in config.get("targets", []):
        domain = target.get("domain")
        sitemap_url = target.get("sitemap_url")

        if not domain or not sitemap_url:
            logger.warning(f"Skipping target with missing domain or sitemap_url: {target}")
            continue

        # 5.3.1 Check if this domain should run today
        if not should_run_domain(target, config):
            continue

        # 5.3.2 Wrap domain processing in try-except for fault tolerance
        try:
            # 5.3.2a Skip jitter for Bankrate (own property) and when disabled
            # Jitter only applies to competitor domains in automated runs
            is_bankrate = any(bd in domain for bd in BANKRATE_DOMAINS)
            jitter_enabled = stealth_config.get("enabled", False) and not is_bankrate
            
            if jitter_enabled:
                jitter_seconds = calculate_startup_jitter(domain, stealth_config)
                if jitter_seconds > 0:
                    logger.info(f"Startup jitter: {jitter_seconds}s for {domain}")
                    time.sleep(jitter_seconds)

            logger.info(f"Processing domain: {domain}, sitemap URL: {sitemap_url}")

            # 5.3.3 Create fetcher with appropriate user agent
            user_agent = get_user_agent(config, domain)
            fetcher_config = {**config, "user_agent": user_agent}
            sitemap_fetcher = SitemapFetcher(config=fetcher_config)
            sitemap_parser = SitemapParser()

            # 5.3.4 Collect sitemap file metadata
            processed_sitemap_urls_for_domain = set()
            sitemap_file_records: List[Dict[str, Any]] = []

            # 5.3.5 Recursively fetch all page URLs
            all_page_url_dicts = process_single_sitemap_url(
                sitemap_url=sitemap_url,
                fetcher=sitemap_fetcher,
                parser=sitemap_parser,
                processed_sitemap_urls=processed_sitemap_urls_for_domain,
                domain=domain,
                sitemap_file_records=sitemap_file_records,
            )

            if not all_page_url_dicts:
                logger.warning(f"No page URLs found for {domain} from {sitemap_url}. Skipping.")
                domain_results[domain] = {"status": "warning", "message": "No URLs found"}
                continue

            logger.info(f"Gathered {len(all_page_url_dicts)} page URLs for {domain}")
            logger.info(f"Processed {len(sitemap_file_records)} sitemap files for {domain}")

            # 5.3.6 Save sitemap file metadata
            if sitemap_file_records:
                data_processor.save_sitemap_metadata(domain, sitemap_file_records)

            # 5.3.7 Log sample for diagnostics
            if all_page_url_dicts:
                sample = all_page_url_dicts[0]
                logger.debug(f"Sample URL: {sample.get('loc')}, section: {sample.get('section')}")

            # 5.3.8 Process URLs and track changes
            urls_df = data_processor.process_sitemap_urls(domain, all_page_url_dicts)

            logger.info(f"Completed processing for domain: {domain}")
            domain_results[domain] = {"status": "success", "urls": len(all_page_url_dicts)}
            
        except Exception as e:
            # 5.3.9 Log error but continue to next domain
            logger.error(f"FAILED processing domain {domain}: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            domain_results[domain] = {"status": "error", "message": str(e)}
            # Continue to next domain instead of crashing
            continue
        
        logger.info("-" * 40)
    
    # 5.4 Summary of domain results
    logger.info("=" * 60)
    logger.info("Domain Processing Summary:")
    for domain, result in domain_results.items():
        status = result.get("status", "unknown")
        if status == "success":
            logger.info(f"  ✓ {domain}: {result.get('urls', 0)} URLs processed")
        elif status == "warning":
            logger.warning(f"  ⚠ {domain}: {result.get('message', 'warning')}")
        else:
            logger.error(f"  ✗ {domain}: {result.get('message', 'failed')}")

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
