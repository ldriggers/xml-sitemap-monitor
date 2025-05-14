import logging
import json # For main's example if config.py is not found
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any

# Project-specific imports
from src.config import load_config, CONFIG_FILE_PATH # Assuming config.py is in src
from src.sitemap_fetcher import SitemapFetcher
from src.sitemap_parser import SitemapParser
from src.data_processor import DataProcessor, COL_URL # Import COL_URL for consistency

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__) # Get logger for the main module

def process_single_sitemap_url(
    sitemap_url: str, 
    fetcher: SitemapFetcher, 
    parser: SitemapParser,
    processed_sitemap_urls: set # To avoid re-processing if a sitemap URL appears multiple times
) -> list:
    """
    Fetches and parses a single sitemap URL (which could be an index or a urlset).
    If it's an index, it recursively calls itself for sub-sitemaps.
    Returns a list of all page URL dictionaries found.
    """
    if sitemap_url in processed_sitemap_urls:
        logger.info(f"Sitemap {sitemap_url} already processed in this run. Skipping.")
        return []
    
    logger.info(f"Processing sitemap: {sitemap_url}")
    processed_sitemap_urls.add(sitemap_url)
    xml_content = fetcher.fetch_sitemap_xml(sitemap_url)
    
    if not xml_content:
        logger.warning(f"Failed to fetch XML content for {sitemap_url}. Skipping.")
        return []

    parsed_data = parser.parse_sitemap(xml_content, sitemap_url=sitemap_url)
    all_page_urls_from_this_branch = []

    if parsed_data["type"] == "sitemapindex":
        logger.info(f"Sitemap index {sitemap_url} contains {len(parsed_data.get('urls', []))} sub-sitemaps.")
        sub_sitemap_urls = parsed_data.get("urls", [])
        for sub_url in sub_sitemap_urls:
            all_page_urls_from_this_branch.extend(
                process_single_sitemap_url(sub_url, fetcher, parser, processed_sitemap_urls)
            )
    elif parsed_data["type"] == "urlset":
        page_urls_in_set = parsed_data.get("urls", [])
        logger.info(f"URL set {sitemap_url} contains {len(page_urls_in_set)} page URLs.")
        all_page_urls_from_this_branch.extend(page_urls_in_set)
    elif parsed_data["type"] == "error":
        logger.error(f"Error parsing sitemap {sitemap_url}: {parsed_data.get('error_message')}")
    else:
        logger.warning(f"Unknown sitemap type '{parsed_data['type']}' for {sitemap_url}. This should not happen.")
        
    return all_page_urls_from_this_branch

def main():
    """Main function to orchestrate the sitemap processing pipeline."""
    logger.info("Starting sitemap processing pipeline")
    
    # Load configuration
    config = load_config()
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return
    
    # Initialize components
    data_dir = config.get("data_directory", "data")
    os.makedirs(data_dir, exist_ok=True)
    
    sitemap_fetcher = SitemapFetcher(config=config)
    sitemap_parser = SitemapParser()
    data_processor = DataProcessor(data_dir=data_dir)
    
    # Process each target domain
    for target in config.get("targets", []):
        domain = target.get("domain")
        sitemap_url = target.get("sitemap_url")
        
        if not domain or not sitemap_url:
            logger.warning(f"Skipping target with missing domain or sitemap_url: {target}")
            continue
        
        logger.info(f"Processing domain: {domain}, initial sitemap URL: {sitemap_url}")
        
        processed_sitemap_urls_for_domain = set() # Keep track of processed sitemaps for this domain
        
        # Use process_single_sitemap_url to recursively fetch all page URLs
        all_page_url_dicts = process_single_sitemap_url(
            sitemap_url=sitemap_url, # The initial sitemap URL from config
            fetcher=sitemap_fetcher,
            parser=sitemap_parser,
            processed_sitemap_urls=processed_sitemap_urls_for_domain
        )
        
        if not all_page_url_dicts:
            logger.warning(f"No page URLs found for domain {domain} from sitemap {sitemap_url} after (recursive) processing. Skipping.")
            continue
            
        # DIAGNOSTIC LOGGING START
        logger.info(f"DIAGNOSTIC: Gathered {len(all_page_url_dicts)} items for domain {domain} from {sitemap_url}.")
        if all_page_url_dicts:
            logger.info(f"DIAGNOSTIC: First item type: {type(all_page_url_dicts[0])}")
            logger.info(f"DIAGNOSTIC: First few items: {str(all_page_url_dicts[:5])}") # Convert to string for logging
        else:
            logger.info(f"DIAGNOSTIC: all_page_url_dicts is empty for domain {domain} from {sitemap_url}.")
        # DIAGNOSTIC LOGGING END
            
        logger.info(f"Successfully gathered {len(all_page_url_dicts)} page URLs for {domain} from {sitemap_url} (and its children if it was an index).")
        
        # Process URLs and track changes in one step
        # Now, all_page_url_dicts should be a list of dictionaries like [{'loc': ..., 'lastmod': ...}, ...]
        urls_df = data_processor.process_sitemap_urls(domain, all_page_url_dicts)
        
        logger.info(f"Completed processing for domain: {domain}")
    
    logger.info("Sitemap processing pipeline completed")

if __name__ == "__main__":
    # This allows running main.py directly from the src/ directory 
    # or from the project root (competitive_content_monitoring/)
    # It tries to adjust CWD if needed for config.json loading by config.py
    
    # Get the directory of the main.py script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir) # Assumes src is one level down from project root
    
    # Check if config.json is in project_root, if so, change CWD to project_root
    # This helps config.py find config.json if main.py is run from src/
    potential_config_path_at_root = os.path.join(project_root, CONFIG_FILE_PATH)
    
    if os.getcwd() == script_dir and os.path.exists(potential_config_path_at_root):
        logger.info(f"Running from src/, changing CWD to project root: {project_root} for config loading.")
        os.chdir(project_root)
    elif not os.path.exists(CONFIG_FILE_PATH):
        # If still not found (e.g. PWD is some other dir), try to locate relative to script
        if os.path.exists(potential_config_path_at_root):
            logger.info(f"Config not in CWD, but found at project root. Changing CWD to: {project_root}")
            os.chdir(project_root)
        else:
            logger.warning(f"Config file {CONFIG_FILE_PATH} not found in CWD ({os.getcwd()}) or expected project root ({project_root}). load_config might fail.")

    main() 