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
            'log': os.path.join(self.data_dir, f"{domain}_processing.log"), # Standard text log
            'change_log_csv': os.path.join(self.data_dir, f"{domain}_changes_history.csv") # New historical change log
        }
    
    def _save_change_log(self, changes_df: pd.DataFrame, change_log_path: str) -> None:
        """Appends detected changes to a CSV log file."""
        if changes_df.empty:
            logger.debug("No changes detected to log in historical change log.")
            return

        try:
            # Define column order for the change log, focusing on essential fields first
            change_log_columns = [
                'detected_at', 'domain', 'loc', 'change_type', 
                'lastmod', 'lastmod_prev', 'sitemap_source_url',
                # Optional sitemap fields - kept for completeness but not essential
                'priority', 'priority_prev', 
                'changefreq', 'changefreq_prev'
            ]
            
            # Ensure all necessary columns exist in changes_df, fill with None if not, and reorder
            final_changes_df = pd.DataFrame(columns=change_log_columns)
            for col in change_log_columns:
                if col in changes_df.columns:
                    final_changes_df[col] = changes_df[col]
                else:
                    final_changes_df[col] = None # Ensure column exists, even if all values are None for it
            
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
        
        Core fields are 'loc' (URL) and 'lastmod'; other fields are optional.
        """
        logger.info(f"Processing {len(sitemap_urls)} URLs for domain: {domain}")
        file_paths = self._get_file_paths(domain)
        current_dt = datetime.now(timezone.utc) # Use timezone-aware UTC timestamp

        # Define canonical columns for the main snapshot data
        # Primary fields first (essential), then secondary fields (optional)
        snapshot_columns = [
            # Primary fields (essential)
            'loc', 'domain', 'lastmod', 'detected_at', 'change_type', 'sitemap_source_url',
            # Secondary fields (optional)
            'changefreq', 'priority'
        ]

        # Create DataFrame from current sitemap URLs
        if not sitemap_urls:
            logger.info(f"No URLs provided in current sitemap fetch for {domain}.")
            # Ensure schema for empty df with essential columns
            current_sitemap_df = pd.DataFrame(columns=['loc', 'lastmod'])
        else:
            current_sitemap_df = pd.DataFrame(sitemap_urls)
            
            # Check for required fields
            if 'loc' not in current_sitemap_df.columns:
                logger.error(f"No 'loc' (URL) field in sitemap data for {domain}. Cannot process.")
                return pd.DataFrame(columns=snapshot_columns)
                
            # Add missing columns as needed, with essential fields first
            for col in ['loc', 'lastmod']:  # Essential fields
                if col not in current_sitemap_df.columns:
                    logger.warning(f"Essential column '{col}' missing in provided sitemap_urls for {domain}. Adding as None.")
                    current_sitemap_df[col] = None
                    
            # Add optional fields if missing
            for col in ['changefreq', 'priority', 'sitemap_source_url']:  # Optional fields
                if col not in current_sitemap_df.columns:
                    current_sitemap_df[col] = None

        current_sitemap_df['domain'] = domain
        current_sitemap_df['detected_at'] = current_dt

        all_changes_records = [] # To store records for the historical change log

        # Load previous data snapshot
        previous_df = pd.DataFrame() # Initialize as empty DataFrame
        parquet_file_path = file_paths['parquet']
        if os.path.exists(parquet_file_path):
            try:
                previous_df = pd.read_parquet(parquet_file_path)
                # Ensure previous_df has essential columns for comparison
                for col in ['loc', 'lastmod']:  # Essential columns
                    if col not in previous_df.columns:
                        logger.warning(f"Essential column '{col}' missing in previous data. Adding as None.")
                        previous_df[col] = None
                        
                # Add other columns if missing (both essential and optional for schema consistency)
                for col in ['domain', 'detected_at', 'change_type', 'sitemap_source_url', 'priority', 'changefreq']:
                    if col not in previous_df.columns:
                        previous_df[col] = None
                        
                logger.info(f"Loaded previous data with {len(previous_df)} URLs for comparison from {parquet_file_path}")
            except Exception as e:
                logger.warning(f"Could not load previous data from {parquet_file_path}: {e}. Treating as no previous data.")
        
        interim_output_df = pd.DataFrame() # To build the next snapshot

        if previous_df.empty:
            logger.info(f"No valid previous data found for {domain}. All {len(current_sitemap_df)} current URLs (if any) will be marked as 'new'.")
            output_df_rows = []
            for _, row in current_sitemap_df.iterrows():
                # Ensure 'loc' is not None before proceeding for this record
                loc_val = row.get('loc')
                if pd.isna(loc_val):
                    logger.warning(f"Skipping record with missing 'loc' in current_sitemap_df for domain {domain}")
                    continue

                # Build the change record with essential fields first
                change_record = {
                    # Essential fields
                    'loc': loc_val, 
                    'domain': domain, 
                    'detected_at': current_dt, 
                    'change_type': 'new',
                    'lastmod': row.get('lastmod'), 
                    'lastmod_prev': None,
                    'sitemap_source_url': row.get('sitemap_source_url'),
                    # Optional fields
                    'priority': row.get('priority'), 
                    'priority_prev': None,
                    'changefreq': row.get('changefreq'), 
                    'changefreq_prev': None
                }
                all_changes_records.append(change_record)
                
                # For snapshot, build record with essential fields first
                output_df_rows.append({
                    # Essential fields
                    'loc': loc_val, 
                    'domain': domain, 
                    'detected_at': current_dt, 
                    'change_type': 'new',
                    'lastmod': row.get('lastmod'),
                    'sitemap_source_url': row.get('sitemap_source_url'),
                    # Optional fields
                    'priority': row.get('priority'), 
                    'changefreq': row.get('changefreq')
                })
            
            if output_df_rows:
                interim_output_df = pd.DataFrame(output_df_rows)
            else:
                interim_output_df = pd.DataFrame(columns=snapshot_columns) # Ensure schema if empty

        else: # Previous data exists, perform comparison
            # Focus on essential fields for the merge, but include optional fields for completeness
            essential_fields = ['loc', 'lastmod'] 
            optional_fields = ['priority', 'changefreq', 'sitemap_source_url']
            
            # Ensure the minimum required columns exist in previous_df for the comparison
            if 'loc' not in previous_df.columns:
                logger.error(f"Previous data for {domain} is missing the 'loc' field. Cannot compare.")
                previous_df['loc'] = None  # Add to avoid errors, but comparison will be limited
            
            # Create rename map for previous data columns used in merge
            rename_map = {f: f"{f}_prev" for f in essential_fields + optional_fields if f != 'loc'}
            
            # Select columns to use from previous_df, based on what's available
            prev_columns = ['loc'] + [col for col in essential_fields + optional_fields if col in previous_df.columns and col != 'loc']
            
            # Perform the merge with focus on essential fields
            merged_df = current_sitemap_df.merge(
                previous_df[prev_columns].rename(columns=rename_map),
                on='loc',
                how='outer', 
                indicator=True 
            )

            output_df_rows = [] 
            
            for _, row in merged_df.iterrows():
                loc = row['loc']
                if pd.isna(loc):
                    logger.warning(f"Skipping record with missing 'loc' during merge for domain {domain}")
                    continue

                current_lastmod = row.get('lastmod')
                prev_lastmod = row.get('lastmod_prev')
                current_priority = row.get('priority')
                prev_priority = row.get('priority_prev')
                current_changefreq = row.get('changefreq')
                prev_changefreq = row.get('changefreq_prev')
                
                sitemap_source = row.get('sitemap_source_url') if pd.notna(row.get('sitemap_source_url')) else row.get('sitemap_source_url_prev')

                change_data_base = {'loc': loc, 'domain': domain, 'detected_at': current_dt, 'sitemap_source_url': sitemap_source}
                snapshot_data_base = {'loc': loc, 'domain': domain, 'detected_at': current_dt, 'sitemap_source_url': sitemap_source}

                if row['_merge'] == 'left_only': 
                    change_type = 'new'
                    all_changes_records.append({**change_data_base, 'change_type': change_type, 
                                                'lastmod': current_lastmod, 'lastmod_prev': None,
                                                'priority': current_priority, 'priority_prev': None,
                                                'changefreq': current_changefreq, 'changefreq_prev': None})
                    output_df_rows.append({**snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod,
                                           'priority': current_priority, 'changefreq': current_changefreq})
                elif row['_merge'] == 'right_only': 
                    change_type = 'removed'
                    all_changes_records.append({**change_data_base, 'change_type': change_type, 
                                                'lastmod': None, 'lastmod_prev': prev_lastmod,
                                                'priority': None, 'priority_prev': prev_priority,
                                                'changefreq': None, 'changefreq_prev': prev_changefreq})
                    output_df_rows.append({**snapshot_data_base, 'change_type': change_type, 'lastmod': prev_lastmod,
                                           'priority': prev_priority, 'changefreq': prev_changefreq})
                elif row['_merge'] == 'both': 
                    updated = False
                    
                    # First check the essential lastmod field
                    if current_lastmod != prev_lastmod and not (pd.isna(current_lastmod) and pd.isna(prev_lastmod)):
                        updated = True
                        
                    # Then check optional fields only if lastmod didn't indicate a change
                    if not updated:
                        # Check priority if available
                        if current_priority != prev_priority and not (pd.isna(current_priority) and pd.isna(prev_priority)):
                            updated = True
                        # Check changefreq if available  
                        if current_changefreq != prev_changefreq and not (pd.isna(current_changefreq) and pd.isna(prev_changefreq)):
                            updated = True
                    
                    if updated:
                        change_type = 'updated'
                        all_changes_records.append({**change_data_base, 'change_type': change_type,
                                                    'lastmod': current_lastmod, 'lastmod_prev': prev_lastmod,
                                                    'priority': current_priority, 'priority_prev': prev_priority,
                                                    'changefreq': current_changefreq, 'changefreq_prev': prev_changefreq})
                        output_df_rows.append({**snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod,
                                               'priority': current_priority, 'changefreq': current_changefreq})
                    else:
                        change_type = 'unchanged'
                        output_df_rows.append({**snapshot_data_base, 'change_type': change_type, 'lastmod': current_lastmod,
                                               'priority': current_priority, 'changefreq': current_changefreq})
            
            if output_df_rows:
                interim_output_df = pd.DataFrame(output_df_rows)
            else: # Should not happen if merged_df had rows, but as failsafe
                interim_output_df = pd.DataFrame(columns=snapshot_columns)

        # Finalize the snapshot DataFrame (output_df)
        output_df = pd.DataFrame(columns=snapshot_columns) # Ensure schema from the start
        if not interim_output_df.empty:
            for col in snapshot_columns:
                if col in interim_output_df.columns:
                    output_df[col] = interim_output_df[col]
                # else: output_df[col] will be all NaN / None by default if not assigned
        
        num_new = len([r for r in all_changes_records if r.get('change_type') == 'new'])
        num_updated = len([r for r in all_changes_records if r.get('change_type') == 'updated'])
        num_removed = len([r for r in all_changes_records if r.get('change_type') == 'removed'])
        logger.info(f"Detected changes for {domain}: {num_new} new, {num_updated} updated, {num_removed} removed.")

        changes_log_df = pd.DataFrame(all_changes_records)
        self._save_change_log(changes_log_df, file_paths['change_log_csv'])

        if not output_df.empty:
            self._save_data(output_df, file_paths['parquet'], file_paths['csv'], file_paths['json'])
            logger.info(f"Saved snapshot with {len(output_df)} URLs for {domain} to {file_paths['parquet']} and other formats.")
        elif os.path.exists(file_paths['parquet']) or os.path.exists(file_paths['csv']) or os.path.exists(file_paths['json']):
            empty_snapshot_df = pd.DataFrame(columns=snapshot_columns)
            self._save_data(empty_snapshot_df, file_paths['parquet'], file_paths['csv'], file_paths['json'])
            logger.info(f"Saved empty snapshot for {domain} as no current URLs were found (previous files overwritten).")
        else:
            logger.info(f"No data to save for snapshot for {domain} and no previous files existed.")
            
        return output_df
    
    def _save_data(self, df: pd.DataFrame, parquet_path: str, csv_path: str, json_path: str) -> None:
        """Save the DataFrame in multiple formats (snapshot - overwrites)."""
        df.to_parquet(parquet_path, index=False)
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient='records', lines=True)
        logger.debug(f"Saved data to {parquet_path}, {csv_path}, and {json_path}")

# Removed Example Usage section 