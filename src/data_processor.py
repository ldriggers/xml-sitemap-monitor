import pandas as pd
import os
import logging
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Define column names to ensure consistency
COL_URL = "url"
COL_DOMAIN = "domain"
COL_COMPETITOR_NAME = "competitor_name"
COL_FIRST_SEEN_AT = "first_seen_at"
COL_LAST_SEEN_AT = "last_seen_at"
COL_LASTMOD = "lastmod"
COL_CHANGEFREQ = "changefreq"
COL_PRIORITY = "priority"
COL_SITEMAP_SOURCE = "sitemap_source" # URL of the sitemap file where the URL was found
COL_IS_ACTIVE = "is_active" # To mark if URL is currently in any sitemap

# For changes log
COL_CHANGE_TYPE = "change_type"
COL_OLD_LASTMOD = "old_lastmod"
COL_NEW_LASTMOD = "new_lastmod"
COL_DETECTED_AT = "detected_at"


class DataProcessor:
    def __init__(self, data_dir: str = "data"):
        """Initialize the data processor with a directory for data storage."""
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"DataProcessor initialized with data directory: {data_dir}")
        
    def _get_file_paths(self, domain: str) -> Dict[str, str]:
        """Generate file paths for different formats based on domain name."""
        base_path = os.path.join(self.data_dir, f"{domain}_urls")
        
        return {
            'parquet': f"{base_path}.parquet",
            'csv': f"{base_path}.csv",
            'json': f"{base_path}.json",
            'log': os.path.join(self.data_dir, f"{domain}_processing.log")
        }
    
    def process_sitemap_urls(self, domain: str, sitemap_urls: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Process and store sitemap URLs for a specific domain, tracking changes.
        
        Args:
            domain: The domain name (e.g., 'bankrate.com')
            sitemap_urls: List of URL dictionaries from parsed sitemap
            
        Returns:
            DataFrame of processed URLs with change information
        """
        logger.info(f"Processing {len(sitemap_urls)} URLs for domain: {domain}")
        file_paths = self._get_file_paths(domain)
        
        # Create DataFrame with current URLs
        current_df = pd.DataFrame(sitemap_urls)
        current_df['domain'] = domain
        current_df['detected_at'] = datetime.now()
        
        # Try to load previous data
        previous_df = None
        if os.path.exists(file_paths['parquet']):
            try:
                previous_df = pd.read_parquet(file_paths['parquet'])
                logger.info(f"Loaded previous data with {len(previous_df)} URLs for comparison")
            except Exception as e:
                logger.warning(f"Could not load previous data: {e}")
        
        # If no previous data, all current URLs are new
        if previous_df is None or len(previous_df) == 0:
            current_df['change_type'] = 'new'
            logger.info(f"No previous data found. All {len(current_df)} URLs marked as new.")
        else:
            # Identify new and updated URLs
            merged = current_df.merge(
                previous_df[['loc', 'lastmod']],
                on='loc', 
                how='left', 
                suffixes=('', '_prev')
            )
            
            # New URLs don't have a previous lastmod
            new_mask = merged['lastmod_prev'].isna()
            # Updated URLs have a different lastmod
            updated_mask = (~new_mask) & (merged['lastmod'] != merged['lastmod_prev']) & (~merged['lastmod'].isna())
            
            current_df['change_type'] = 'unchanged'
            current_df.loc[new_mask, 'change_type'] = 'new'
            current_df.loc[updated_mask, 'change_type'] = 'updated'
            
            # Identify removed URLs (in previous but not in current)
            current_locs = set(current_df['loc'])
            previous_locs = set(previous_df['loc'])
            removed_locs = previous_locs - current_locs
            
            if removed_locs:
                removed_df = previous_df[previous_df['loc'].isin(removed_locs)].copy()
                removed_df['change_type'] = 'removed'
                removed_df['detected_at'] = datetime.now()
                current_df = pd.concat([current_df, removed_df])
                logger.info(f"Detected {len(removed_df)} removed URLs")
            
            logger.info(f"Detected changes: {new_mask.sum()} new, {updated_mask.sum()} updated, {len(removed_locs)} removed")
        
        # Save in all formats
        self._save_data(current_df, file_paths['parquet'], file_paths['csv'], file_paths['json'])
        logger.info(f"Saved {len(current_df)} URLs for {domain}")
        
        return current_df
    
    def _save_data(self, df: pd.DataFrame, parquet_path: str, csv_path: str, json_path: str) -> None:
        """Save the DataFrame in multiple formats."""
        df.to_parquet(parquet_path, index=False)
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', lines=True)
        logger.debug(f"Saved data to {parquet_path}, {csv_path}, and {json_path}")

# Example Usage (for testing this module directly)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a dummy data directory for testing
    test_data_dir = "test_data_processor"
    if not os.path.exists(test_data_dir):
        os.makedirs(test_data_dir)

    processor = DataProcessor(data_dir=test_data_dir)

    # Mock domain config
    domain_cfg = {"name": "TestCorp", "domain": "testcorp.com", "sitemap_url": "http://testcorp.com/sitemap.xml"}
    sitemap_file_url = "http://testcorp.com/sitemap_part1.xml"

    # Test 1: First run for a domain
    logger.info("--- Test 1: First Run ---")
    first_run_urls = [
        {'loc': 'http://testcorp.com/page1', 'lastmod': '2023-01-01T00:00:00Z'},
        {'loc': 'http://testcorp.com/page2', 'lastmod': '2023-01-02T00:00:00Z'}
    ]
    processor.process_sitemap_urls(domain_cfg["domain"], first_run_urls)
    df_urls = processor.process_sitemap_urls(domain_cfg["domain"], first_run_urls)
    assert len(df_urls) == 2
    logger.info(f"URLs after first run: {len(df_urls)}")
    logger.info(f"First URL 'is_active': {df_urls[df_urls[COL_URL] == 'http://testcorp.com/page1'][COL_IS_ACTIVE].iloc[0]}")


    # Test 2: Second run with updates, a new URL, and one URL removed from this sitemap file
    logger.info("--- Test 2: Second Run (updates, new, removed from file) ---")
    second_run_urls = [
        {'loc': 'http://testcorp.com/page1', 'lastmod': '2023-01-01T12:00:00Z'}, # Updated lastmod
        # page2 is now missing from this sitemap file
        {'loc': 'http://testcorp.com/page3', 'lastmod': '2023-01-03T00:00:00Z'}  # New URL
    ]
    processor.process_sitemap_urls(domain_cfg["domain"], second_run_urls)
    df_urls = processor.process_sitemap_urls(domain_cfg["domain"], second_run_urls)
    # Expected: 3 URLs total (page1, page2, page3). page2 becomes inactive if this was its only source.
    # Changes: 1 new (page3), 1 updated (page1), 1 removed_from_sitemap_file (page2), 1 globally removed (page2)
    logger.info(f"URLs after second run: {len(df_urls)}") # Should be 2 (new)+1 (update)+1 (removed_from_sitemap_file) + 1 (globally removed) = 5 changes
    assert len(df_urls) == 3 # page1, page2, page3 are known
    
    page1_details = df_urls[df_urls[COL_URL] == 'http://testcorp.com/page1'].iloc[0]
    page2_details = df_urls[df_urls[COL_URL] == 'http://testcorp.com/page2'].iloc[0]
    page3_details = df_urls[df_urls[COL_URL] == 'http://testcorp.com/page3'].iloc[0]
    
    assert page1_details[COL_LASTMOD] == '2023-01-01T12:00:00Z'
    assert page1_details[COL_IS_ACTIVE] == True
    assert page2_details[COL_IS_ACTIVE] == False # page2 was not in second_run_urls
    assert page3_details[COL_IS_ACTIVE] == True

    # Check changes log for page2 removal
    removed_change_page2 = df_urls[
        (df_urls[COL_URL] == 'http://testcorp.com/page2') &
        (df_urls[COL_CHANGE_TYPE] == 'removed')
    ]
    assert not removed_change_page2.empty

    # Test 3: A URL previously removed from a sitemap file is seen again in THE SAME sitemap file
    logger.info("--- Test 3: Reactivation in same sitemap file ---")
    third_run_urls = [
        {'loc': 'http://testcorp.com/page1', 'lastmod': '2023-01-01T12:00:00Z'}, 
        {'loc': 'http://testcorp.com/page2', 'lastmod': '2023-01-04T00:00:00Z'}, # page2 reappears
        {'loc': 'http://testcorp.com/page3', 'lastmod': '2023-01-03T00:00:00Z'} 
    ]
    processor.process_sitemap_urls(domain_cfg["domain"], third_run_urls)
    df_urls = processor.process_sitemap_urls(domain_cfg["domain"], third_run_urls)
    
    page2_details_reactivated = df_urls[df_urls[COL_URL] == 'http://testcorp.com/page2'].iloc[0]
    assert page2_details_reactivated[COL_IS_ACTIVE] == True
    assert page2_details_reactivated[COL_LASTMOD] == '2023-01-04T00:00:00Z'
    
    reactivated_change_page2 = df_urls[
        (df_urls[COL_URL] == 'http://testcorp.com/page2') &
        ((df_urls[COL_CHANGE_TYPE] == 'reactivated') | (df_urls[COL_CHANGE_TYPE] == 'updated')) # Could be updated if lastmod also changed
    ]
    assert not reactivated_change_page2.empty
    logger.info(f"Page 2 is_active after reactivation: {page2_details_reactivated[COL_IS_ACTIVE]}")
    logger.info(f"Total changes after Test 3: {len(df_urls)}")

    # Clean up test data directory
    # import shutil
    # shutil.rmtree(test_data_dir)
    # logger.info(f"Cleaned up test data directory: {test_data_dir}")
    logger.info("DataProcessor testing complete. Test data saved in 'test_data_processor' directory.") 