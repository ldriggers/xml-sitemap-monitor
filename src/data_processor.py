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
# COL_CHANGEFREQ = "changefreq" # No longer used
# COL_PRIORITY = "priority" # No longer used
COL_SITEMAP_SOURCE = "sitemap_source" # URL of the sitemap file where the URL was found
COL_IS_ACTIVE = "is_active" # To mark if URL is currently in any sitemap

# For changes log
COL_CHANGE_TYPE = "change_type"
COL_OLD_LASTMOD = "old_lastmod"
COL_NEW_LASTMOD = "new_new_lastmod" # Typo in original, but will be superseded by code logic
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
            'log': os.path.join(self.data_dir, f"{domain}_processing.log"),
            'change_log_csv': os.path.join(self.data_dir, f"{domain}_changes_history.csv")
        }
    
    def _save_change_log(self, changes_df: pd.DataFrame, change_log_path: str) -> None:
        """Appends detected changes to a CSV log file."""
        if changes_df.empty:
            return

        try:
            # Define column order for the change log, excluding priority and changefreq
            change_log_columns = [
                'detected_at', 'domain', 'loc', 'change_type', 
                'lastmod', 'lastmod_prev', 'sitemap_source_url'
            ]
            
            final_changes_df = pd.DataFrame(columns=change_log_columns)
            for col in change_log_columns:
                if col in changes_df.columns:
                    final_changes_df[col] = changes_df[col]
                else:
                    final_changes_df[col] = None
            
            if os.path.exists(change_log_path):
                final_changes_df.to_csv(change_log_path, mode='a', header=False, index=False)
                logger.info(f"Appended {len(final_changes_df)} changes to {change_log_path}")
            else:
                final_changes_df.to_csv(change_log_path, mode='w', header=True, index=False)
                logger.info(f"Created new change log with {len(final_changes_df)} changes at {change_log_path}")
        except Exception as e:
            logger.error(f"Error saving change log to {change_log_path}: {e}")
    
    def process_sitemap_urls(self, domain: str, sitemap_urls: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Process and store sitemap URLs for a specific domain, tracking changes.
        Saves a snapshot of the current state and appends changes to a historical log.
        If the historical log doesn't exist, it's seeded with the current snapshot data.
        Only 'loc' and 'lastmod' (and sitemap_source_url) are processed and stored.
        """
        logger.info(f"Processing {len(sitemap_urls)} URLs for domain: {domain} (priority/changefreq excluded)")
        file_paths = self._get_file_paths(domain)
        current_dt = datetime.now(timezone.utc)

        # Define canonical columns for the main snapshot data, excluding priority and changefreq
        snapshot_columns = [
            'loc', 'domain', 'lastmod', 'detected_at', 'change_type', 'sitemap_source_url'
        ]
        change_log_csv_path = file_paths['change_log_csv']
        parquet_file_path = file_paths['parquet']

        existing_snapshot_df = pd.DataFrame()
        if os.path.exists(parquet_file_path):
            try:
                existing_snapshot_df = pd.read_parquet(parquet_file_path)
                # Ensure essential columns exist for consistent processing
                # We don't care about priority/changefreq from old files anymore
                required_cols_for_snapshot_load = ['loc', 'lastmod', 'sitemap_source_url', 'domain', 'detected_at', 'change_type']
                for col in required_cols_for_snapshot_load:
                    if col not in existing_snapshot_df.columns:
                        existing_snapshot_df[col] = None
                logger.info(f"Loaded existing snapshot with {len(existing_snapshot_df)} URLs from {parquet_file_path}.")
            except Exception as e:
                logger.warning(f"Could not load existing snapshot from {parquet_file_path}: {e}. Proceeding as if no previous snapshot.")

        if not os.path.exists(change_log_csv_path):
            logger.info(f"Historical change log {change_log_csv_path} not found.")
            if not existing_snapshot_df.empty:
                logger.info(f"Backfilling historical log with {len(existing_snapshot_df)} URLs from snapshot, marking them 'new'.")
                backfill_changes_records = []
                for _, row in existing_snapshot_df.iterrows():
                    loc_val = row.get('loc')
                    if pd.isna(loc_val): continue
                    backfill_changes_records.append({
                        'detected_at': current_dt,
                        'domain': row.get('domain', domain),
                        'loc': loc_val,
                        'change_type': 'new',
                        'lastmod': row.get('lastmod'),
                        'lastmod_prev': None,
                        'sitemap_source_url': row.get('sitemap_source_url')
                        # priority and changefreq intentionally omitted
                    })
                if backfill_changes_records:
                    self._save_change_log(pd.DataFrame(backfill_changes_records), change_log_csv_path)
            else:
                logger.info("No existing snapshot to backfill historical log. Log will start from current fetch.")

        # Process live sitemap URLs, focusing only on loc, lastmod, sitemap_source_url
        processed_live_urls = []
        if sitemap_urls:
            for item in sitemap_urls:
                if item and isinstance(item, dict) and item.get('loc'):
                    processed_live_urls.append({
                        'loc': item.get('loc'),
                        'lastmod': item.get('lastmod'),
                        'sitemap_source_url': item.get('sitemap_source_url') # Keep if provided by parser
                    })
        
        current_sitemap_live_df = pd.DataFrame(processed_live_urls)
        if current_sitemap_live_df.empty:
             logger.info(f"No valid URLs (with loc) in current live sitemap fetch for {domain}.")
             # Ensure schema if empty
             current_sitemap_live_df = pd.DataFrame(columns=['loc', 'lastmod', 'sitemap_source_url'])
        
        current_sitemap_live_df['domain'] = domain

        if not current_sitemap_live_df.empty and 'loc' in current_sitemap_live_df.columns:
            num_before_dedupe = len(current_sitemap_live_df)
            if 'lastmod' in current_sitemap_live_df.columns:
                current_sitemap_live_df['lastmod_dt'] = pd.to_datetime(current_sitemap_live_df['lastmod'], errors='coerce')
                current_sitemap_live_df = current_sitemap_live_df.sort_values(
                    by=['loc', 'lastmod_dt'], 
                    ascending=[True, False],
                    na_position='last'
                )
                current_sitemap_live_df = current_sitemap_live_df.drop(columns=['lastmod_dt'])
            else:
                current_sitemap_live_df = current_sitemap_live_df.sort_values(by=['loc'], ascending=[True])
            
            current_sitemap_live_df = current_sitemap_live_df.drop_duplicates(subset=['loc'], keep='first')
            num_after_dedupe = len(current_sitemap_live_df)
            if num_before_dedupe > num_after_dedupe:
                logger.info(f"Deduplicated live sitemap URLs by 'loc': {num_before_dedupe} before, {num_after_dedupe} after for domain {domain}.")

        current_run_changes_records = []
        output_df_rows = []

        if existing_snapshot_df.empty:
            logger.info(f"No existing snapshot for comparison. All {len(current_sitemap_live_df)} live URLs are 'new' for this run.")
            for _, row in current_sitemap_live_df.iterrows():
                loc_val = row.get('loc')
                if pd.isna(loc_val): continue
                current_run_changes_records.append({
                    'detected_at': current_dt, 'domain': domain, 'loc': loc_val, 'change_type': 'new',
                    'lastmod': row.get('lastmod'), 'lastmod_prev': None,
                    'sitemap_source_url': row.get('sitemap_source_url')
                    # priority and changefreq intentionally omitted
                })
                output_df_rows.append({
                    'loc': loc_val, 'domain': domain, 'lastmod': row.get('lastmod'), 
                    'detected_at': current_dt, 'change_type': 'new', 
                    'sitemap_source_url': row.get('sitemap_source_url')
                    # priority and changefreq intentionally omitted
                })
        else:
            prev_rename_map = {
                'lastmod': 'lastmod_prev', 
                'sitemap_source_url': 'sitemap_source_url_prev'
                # No priority/changefreq in rename map
            }
            # Only select loc, lastmod, sitemap_source_url from existing snapshot for merge comparison
            prev_cols_for_merge = ['loc']
            if 'lastmod' in existing_snapshot_df.columns: prev_cols_for_merge.append('lastmod')
            if 'sitemap_source_url' in existing_snapshot_df.columns: prev_cols_for_merge.append('sitemap_source_url')
            
            # Ensure 'loc' is in current_sitemap_live_df before merge
            if 'loc' not in current_sitemap_live_df.columns:
                 current_sitemap_live_df['loc'] = None 

            merged_df = current_sitemap_live_df.merge(
                existing_snapshot_df[list(set(prev_cols_for_merge))].rename(columns=prev_rename_map), # Use set to ensure unique cols
                on='loc',
                how='outer',
                indicator=True
            )

            for _, row in merged_df.iterrows():
                loc = row.get('loc')
                if pd.isna(loc): continue

                current_lastmod = row.get('lastmod')
                current_sitemap_source = row.get('sitemap_source_url')

                prev_lastmod = row.get('lastmod_prev')
                prev_sitemap_source = row.get('sitemap_source_url_prev')
                
                sitemap_source_for_log = current_sitemap_source if pd.notna(current_sitemap_source) else prev_sitemap_source

                change_data_base = {'detected_at': current_dt, 'domain': domain, 'loc': loc, 'sitemap_source_url': sitemap_source_for_log}
                snapshot_data_base = {'loc': loc, 'domain': domain, 'detected_at': current_dt, 'sitemap_source_url': current_sitemap_source}

                if row['_merge'] == 'left_only':
                    change_type = 'new'
                    current_run_changes_records.append({
                        **change_data_base, 'change_type': change_type, 
                        'lastmod': current_lastmod, 'lastmod_prev': None
                    })
                    output_df_rows.append({
                        **snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod
                    })
                elif row['_merge'] == 'right_only':
                    change_type = 'removed'
                    current_run_changes_records.append({
                        **change_data_base, 'change_type': change_type, 
                        'lastmod': None, 'lastmod_prev': prev_lastmod
                    })
                    output_df_rows.append({
                        'loc': loc, 'domain': domain, 'lastmod': prev_lastmod,
                        'detected_at': current_dt, 'change_type': change_type, 
                        'sitemap_source_url': prev_sitemap_source
                    })
                elif row['_merge'] == 'both':
                    updated = False
                    # Update status is now SOLELY based on lastmod
                    if current_lastmod != prev_lastmod and not (pd.isna(current_lastmod) and pd.isna(prev_lastmod)):
                        updated = True
                    
                    if updated:
                        change_type = 'updated'
                        current_run_changes_records.append({
                            **change_data_base, 'change_type': change_type,
                            'lastmod': current_lastmod, 'lastmod_prev': prev_lastmod
                        })
                        output_df_rows.append({
                            **snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod
                        })
                    else:
                        change_type = 'unchanged'
                        output_df_rows.append({
                            **snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod
                        })
        
        output_df = pd.DataFrame(columns=snapshot_columns)
        if output_df_rows:
            temp_output_df = pd.DataFrame(output_df_rows)
            for col in snapshot_columns:
                if col in temp_output_df.columns:
                    output_df[col] = temp_output_df[col]

        num_new_this_run = len([r for r in current_run_changes_records if r.get('change_type') == 'new'])
        num_updated_this_run = len([r for r in current_run_changes_records if r.get('change_type') == 'updated'])
        num_removed_this_run = len([r for r in current_run_changes_records if r.get('change_type') == 'removed'])
        logger.info(f"Changes in current run (live vs snapshot): {num_new_this_run} new, {num_updated_this_run} updated, {num_removed_this_run} removed.")

        if current_run_changes_records:
            changes_df_current_run = pd.DataFrame(current_run_changes_records)
            self._save_change_log(changes_df_current_run, change_log_csv_path)
        else:
            logger.info(f"No changes from current run to append to historical log {change_log_csv_path}.")

        if not output_df.empty:
            self._save_data(output_df, file_paths['parquet'], file_paths['csv'], file_paths['json'])
            logger.info(f"Saved new snapshot with {len(output_df)} URLs for {domain}.")
        elif os.path.exists(parquet_file_path) or os.path.exists(file_paths['csv']) or os.path.exists(file_paths['json']):
            empty_snapshot_df = pd.DataFrame(columns=snapshot_columns)
            self._save_data(empty_snapshot_df, file_paths['parquet'], file_paths['csv'], file_paths['json'])
            logger.info(f"Saved empty snapshot for {domain} (previous files overwritten).")
        else:
            logger.info(f"No data for snapshot for {domain}; no previous files existed.")
            
        return output_df
    
    def _save_data(self, df: pd.DataFrame, parquet_path: str, csv_path: str, json_path: str) -> None:
        """Save the DataFrame in multiple formats (snapshot - overwrites)."""
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        os.makedirs(os.path.dirname(json_path), exist_ok=True)

        df.to_parquet(parquet_path, index=False)
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', lines=True)
        logger.debug(f"Saved data to {parquet_path}, {csv_path}, and {json_path}")

# Removed Example Usage section 