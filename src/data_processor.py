"""
1.0 Data Processor Module
Handles sitemap URL processing, change detection, and data storage.

Key features:
- Per-domain folder structure with domain-prefixed filenames
- Monthly change log files to prevent size bloat
- All-time URL tracking with current_live vs old_live status
- URL path/section categorization for content analysis
- CSV-only output (removed Parquet/JSON for simplicity)
"""

import pandas as pd
import os
import logging
from glob import glob
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# 1.1 Column name constants for consistency
COL_LOC = "loc"
COL_DOMAIN = "domain"
COL_LASTMOD = "lastmod"
COL_SITEMAP_SOURCE = "sitemap_source_url"
COL_DETECTED_AT = "detected_at"
COL_CHANGE_TYPE = "change_type"
COL_SECTION = "section"
COL_SUBSECTION = "subsection"
COL_PATH_DEPTH = "path_depth"


class DataProcessor:
    """
    2.0 DataProcessor Class
    Processes sitemap URLs, detects changes, and maintains historical records.
    """

    def __init__(self, data_dir: str = "data"):
        """
        2.1 Initialize the data processor.
        
        Args:
            data_dir: Root directory for data storage (default: "data")
        """
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"DataProcessor initialized with data directory: {data_dir}")

    # =========================================================================
    # 3.0 FILE PATH HELPERS
    # =========================================================================

    def _get_file_paths(self, domain: str) -> Dict[str, str]:
        """
        3.1 Generate file paths for URL-level data for a given domain.
        
        Layout (CSV-only):
            data/
                bankrate.com/
                    bankrate.com_urls.csv           (current snapshot)
                    bankrate.com_urls_all_time.csv  (all URLs ever seen)
                    bankrate.com_sitemaps.csv       (sitemap file metadata)
                    bankrate.com_changes_YYYY-MM.csv (monthly changes)
        
        Args:
            domain: The domain name (e.g., "bankrate.com")
            
        Returns:
            Dictionary with file paths
        """
        domain_dir = os.path.join(self.data_dir, domain)
        os.makedirs(domain_dir, exist_ok=True)
        
        return {
            "snapshot_csv": os.path.join(domain_dir, f"{domain}_urls.csv"),
            "all_time_csv": os.path.join(domain_dir, f"{domain}_urls_all_time.csv"),
            "sitemaps_csv": os.path.join(domain_dir, f"{domain}_sitemaps.csv"),
            "domain_dir": domain_dir,
        }

    def _get_monthly_change_log_path(self, domain: str, run_ts: datetime) -> str:
        """
        3.2 Get the path for the monthly change log file.
        """
        month_str = run_ts.strftime("%Y-%m")
        domain_dir = os.path.join(self.data_dir, domain)
        os.makedirs(domain_dir, exist_ok=True)
        return os.path.join(domain_dir, f"{domain}_changes_{month_str}.csv")

    def _has_existing_change_log(self, domain: str) -> bool:
        """
        3.3 Check if any historical change log exists for this domain.
        """
        domain_dir = os.path.join(self.data_dir, domain)
        
        # Check legacy formats
        legacy_patterns = [
            os.path.join(self.data_dir, f"{domain}_changes_history.csv"),
            os.path.join(domain_dir, f"{domain}_changes_history.csv"),
        ]
        for path in legacy_patterns:
            if os.path.exists(path):
                return True
        
        # Check for any monthly files
        monthly_pattern = os.path.join(domain_dir, f"{domain}_changes_*.csv")
        if glob(monthly_pattern):
            return True
        
        return False

    def _load_snapshot(self, snapshot_path: str) -> pd.DataFrame:
        """
        3.4 Load existing snapshot from CSV (or legacy Parquet) with validation.
        
        Validates:
        - Required columns exist
        - No duplicate URLs
        - No null values in key columns
        """
        df = pd.DataFrame()
        
        # Try CSV first
        if os.path.exists(snapshot_path):
            try:
                df = pd.read_csv(snapshot_path)
            except Exception as e:
                logger.warning(f"Could not load CSV snapshot: {e}")
        
        # Try legacy Parquet if CSV not found
        if df.empty:
            parquet_path = snapshot_path.replace('.csv', '.parquet')
            if os.path.exists(parquet_path):
                try:
                    df = pd.read_parquet(parquet_path)
                    logger.info(f"Migrated from legacy Parquet: {parquet_path}")
                except Exception as e:
                    logger.warning(f"Could not load Parquet snapshot: {e}")
        
        # 🆕 VALIDATION: Skip if empty
        if df.empty:
            return df
        
        # 🆕 VALIDATION: Log DataFrame info
        logger.info(
            f"Loaded snapshot: {len(df):,} rows, {df.shape[1]} columns, "
            f"memory: {df.memory_usage(deep=True).sum() / 1024:.1f} KB"
        )
        
        # 🆕 VALIDATION: Check required columns
        required_cols = ['loc']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"Snapshot missing required columns: {missing_cols}")
        
        # 🆕 VALIDATION: Check for duplicates
        if 'loc' in df.columns:
            dup_count = df.duplicated(subset=['loc']).sum()
            if dup_count > 0:
                logger.warning(f"Snapshot has {dup_count:,} duplicate URLs - will dedupe")
                df = df.drop_duplicates(subset=['loc'], keep='first')
        
        # 🆕 VALIDATION: Check for null values in key column
        if 'loc' in df.columns:
            null_count = df['loc'].isna().sum()
            if null_count > 0:
                logger.warning(f"Snapshot has {null_count:,} null 'loc' values - will filter")
                df = df.dropna(subset=['loc'])
        
        # 🆕 VALIDATION: Log data quality summary
        if 'lastmod' in df.columns:
            lastmod_null_pct = df['lastmod'].isna().sum() / len(df) * 100
            if lastmod_null_pct > 50:
                logger.info(f"Note: {lastmod_null_pct:.0f}% of URLs have no lastmod")
        
        return df

    # =========================================================================
    # 4.0 DATA SAVING METHODS
    # =========================================================================

    def _save_change_log(self, changes_df: pd.DataFrame, change_log_path: str) -> None:
        """
        4.1 Append detected changes to a monthly CSV log file.
        
        Uses vectorized column operations instead of loops for better performance.
        Handles schema migrations when new columns are added.
        """
        if changes_df.empty:
            return

        try:
            # Define expected columns (canonical schema)
            # Includes first_seen_at and last_seen_at for URL lifecycle tracking
            change_log_columns = [
                'detected_at', 'domain', 'loc', 'change_type',
                'first_seen_at', 'last_seen_at',
                'lastmod', 'lastmod_prev', 'sitemap_source_url',
                'section', 'subsection', 'path_depth'
            ]

            # Reindex to ensure all columns exist in correct order
            final_df = changes_df.reindex(columns=change_log_columns)

            os.makedirs(os.path.dirname(change_log_path), exist_ok=True)

            if os.path.exists(change_log_path):
                # Check if existing file has matching schema
                try:
                    existing_df = pd.read_csv(change_log_path, nrows=0)
                    existing_cols = list(existing_df.columns)
                    
                    if existing_cols != change_log_columns:
                        # Schema mismatch - migrate existing data to new schema
                        logger.info(f"Migrating {change_log_path} to new schema (adding first_seen_at, last_seen_at)")
                        existing_df = pd.read_csv(change_log_path, low_memory=False)
                        existing_df = existing_df.reindex(columns=change_log_columns)
                        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
                        combined_df.to_csv(change_log_path, mode='w', header=True, index=False)
                        logger.info(f"Migrated and appended {len(final_df):,} changes to {change_log_path}")
                    else:
                        # Schema matches - simple append
                        final_df.to_csv(change_log_path, mode='a', header=False, index=False)
                        logger.info(f"Appended {len(final_df):,} changes to {change_log_path}")
                except Exception as read_err:
                    logger.warning(f"Could not read existing file for schema check: {read_err}")
                    final_df.to_csv(change_log_path, mode='a', header=False, index=False)
            else:
                final_df.to_csv(change_log_path, mode='w', header=True, index=False)
                logger.info(f"Created change log with {len(final_df):,} changes at {change_log_path}")
                
        except Exception as e:
            logger.error(f"Error saving change log: {e}")

    def _save_snapshot(self, df: pd.DataFrame, csv_path: str) -> None:
        """
        4.2 Save snapshot as CSV only.
        """
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path, index=False)
        logger.debug(f"Saved snapshot to {csv_path}")

    def save_sitemap_metadata(self, domain: str, sitemap_records: List[Dict[str, Any]]) -> None:
        """
        4.3 Save sitemap file metadata to CSV.
        
        Tracks each sitemap file with:
        - sitemap_url, domain, sitemap_type
        - url_count, content_hash, content_length
        - fetched_at
        """
        if not sitemap_records:
            return
        
        file_paths = self._get_file_paths(domain)
        sitemaps_path = file_paths["sitemaps_csv"]
        
        df = pd.DataFrame(sitemap_records)
        
        # Reorder columns for readability
        column_order = [
            'sitemap_url', 'domain', 'sitemap_type', 'url_count',
            'content_hash', 'content_length', 'fetched_at'
        ]
        for col in column_order:
            if col not in df.columns:
                df[col] = None
        df = df[[c for c in column_order if c in df.columns]]
        
        df.to_csv(sitemaps_path, index=False)
        logger.info(f"Saved {len(df)} sitemap records to {sitemaps_path}")

    # =========================================================================
    # 5.0 ALL-TIME URL TRACKING
    # =========================================================================

    def _update_all_time_live(self, domain: str, current_snapshot_df: pd.DataFrame) -> pd.DataFrame:
        """
        5.1 Maintain an 'all time' list of URLs for a domain.
        """
        file_paths = self._get_file_paths(domain)
        all_time_path = file_paths["all_time_csv"]
        now = datetime.now(timezone.utc)

        # Normalize current snapshot
        cur = current_snapshot_df.copy() if not current_snapshot_df.empty else pd.DataFrame()
        
        if cur.empty:
            cur = pd.DataFrame(columns=["loc", "lastmod", "sitemap_source_url", "domain", "section"])
        
        if "domain" not in cur.columns:
            cur["domain"] = domain

        # Keep relevant columns
        keep_cols = ["loc", "domain", "lastmod", "sitemap_source_url", "section", "subsection", "path_depth"]
        available_cols = [c for c in keep_cols if c in cur.columns]
        cur = cur[available_cols].dropna(subset=["loc"])
        cur = cur.drop_duplicates(subset=["loc"], keep="first")

        # Load existing all-time file
        all_time_columns = [
            "loc", "domain", "first_seen_at", "last_seen_at",
            "is_current_live", "live_status", "last_lastmod", 
            "last_sitemap_source_url", "section", "subsection", "path_depth"
        ]
        
        if os.path.exists(all_time_path):
            try:
                all_time = pd.read_csv(all_time_path)
            except Exception as e:
                logger.warning(f"Could not load all-time file: {e}")
                all_time = pd.DataFrame(columns=all_time_columns)
        else:
            all_time = pd.DataFrame(columns=all_time_columns)

        # Ensure columns exist
        for col in all_time_columns:
            if col not in all_time.columns:
                all_time[col] = None

        # Index by loc
        if not all_time.empty:
            all_time = all_time.set_index("loc", drop=False)
        if not cur.empty:
            cur = cur.set_index("loc", drop=False)

        # Mark all as not current first
        if not all_time.empty:
            all_time["is_current_live"] = False

        # Update existing URLs
        if not all_time.empty and not cur.empty:
            shared = all_time.index.intersection(cur.index)
            if len(shared) > 0:
                all_time.loc[shared, "is_current_live"] = True
                all_time.loc[shared, "last_seen_at"] = now
                all_time.loc[shared, "last_lastmod"] = cur.loc[shared, "lastmod"]
                if "sitemap_source_url" in cur.columns:
                    all_time.loc[shared, "last_sitemap_source_url"] = cur.loc[shared, "sitemap_source_url"]
                if "section" in cur.columns:
                    all_time.loc[shared, "section"] = cur.loc[shared, "section"]
                if "subsection" in cur.columns:
                    all_time.loc[shared, "subsection"] = cur.loc[shared, "subsection"]
                if "path_depth" in cur.columns:
                    all_time.loc[shared, "path_depth"] = cur.loc[shared, "path_depth"]

        # Add new URLs
        if not cur.empty:
            new_locs = cur.index.difference(all_time.index) if not all_time.empty else cur.index
            
            if len(new_locs) > 0:
                new_rows_data = {
                    "loc": cur.loc[new_locs, "loc"],
                    "domain": cur.loc[new_locs, "domain"],
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "is_current_live": True,
                    "last_lastmod": cur.loc[new_locs, "lastmod"] if "lastmod" in cur.columns else None,
                }
                if "sitemap_source_url" in cur.columns:
                    new_rows_data["last_sitemap_source_url"] = cur.loc[new_locs, "sitemap_source_url"]
                if "section" in cur.columns:
                    new_rows_data["section"] = cur.loc[new_locs, "section"]
                if "subsection" in cur.columns:
                    new_rows_data["subsection"] = cur.loc[new_locs, "subsection"]
                if "path_depth" in cur.columns:
                    new_rows_data["path_depth"] = cur.loc[new_locs, "path_depth"]
                
                new_rows = pd.DataFrame(new_rows_data, index=new_locs)
                all_time = pd.concat([all_time, new_rows], axis=0)

        # Derive live_status
        if not all_time.empty:
            all_time["live_status"] = all_time["is_current_live"].apply(
                lambda v: "current_live" if bool(v) else "old_live"
            )

        # Save - reset index first to avoid ambiguity (loc is both index and column)
        all_time = all_time.reset_index(drop=True)
        all_time = all_time.sort_values(["domain", "loc"]).reset_index(drop=True)
        
        try:
            all_time.to_csv(all_time_path, index=False)
            
            # Summary stats
            current_count = len(all_time[all_time["is_current_live"] == True])
            old_count = len(all_time[all_time["is_current_live"] == False])
            logger.info(f"All-time for {domain}: {len(all_time)} total ({current_count} live, {old_count} old)")
        except Exception as e:
            logger.error(f"Error saving all-time file: {e}")

        return all_time

    # =========================================================================
    # 6.0 MAIN PROCESSING METHOD
    # =========================================================================

    def process_sitemap_urls(self, domain: str, sitemap_urls: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        6.1 Process sitemap URLs for a domain, tracking changes.
        """
        logger.info(f"Processing {len(sitemap_urls)} URLs for domain: {domain}")
        
        file_paths = self._get_file_paths(domain)
        current_dt = datetime.now(timezone.utc)

        # Snapshot columns (including section analysis)
        snapshot_columns = [
            'loc', 'domain', 'lastmod', 'detected_at', 'change_type', 
            'sitemap_source_url', 'section', 'subsection', 'path_depth'
        ]

        change_log_path = self._get_monthly_change_log_path(domain, current_dt)
        snapshot_path = file_paths['snapshot_csv']

        # Load existing snapshot
        existing_df = self._load_snapshot(snapshot_path)
        if not existing_df.empty:
            for col in ['loc', 'lastmod', 'sitemap_source_url', 'change_type', 'section']:
                if col not in existing_df.columns:
                    existing_df[col] = None
            logger.info(f"Loaded existing snapshot: {len(existing_df)} URLs")
        
        # Load all-time data to get first_seen_at for existing URLs
        all_time_path = file_paths['all_time_csv']
        all_time_lookup = {}
        if os.path.exists(all_time_path):
            try:
                all_time_df = pd.read_csv(all_time_path)
                if 'loc' in all_time_df.columns and 'first_seen_at' in all_time_df.columns:
                    all_time_lookup = dict(zip(all_time_df['loc'], all_time_df['first_seen_at']))
                    logger.debug(f"Loaded {len(all_time_lookup)} URLs from all-time for first_seen lookup")
            except Exception as e:
                logger.warning(f"Could not load all-time for lookup: {e}")

        # One-time backfill check
        if not self._has_existing_change_log(domain) and not existing_df.empty:
            logger.info(f"Backfilling change log with {len(existing_df):,} existing URLs")
            
            # 🆕 VECTORIZED: Build backfill DataFrame without iterrows()
            # Filter out null locs first
            backfill_df = existing_df.dropna(subset=['loc']).copy()
            
            if not backfill_df.empty:
                # Add/set columns in bulk (vectorized)
                backfill_df['detected_at'] = current_dt
                backfill_df['domain'] = domain
                backfill_df['change_type'] = 'discovered'
                backfill_df['lastmod_prev'] = None
                
                # Ensure all expected columns exist
                for col in ['sitemap_source_url', 'section', 'subsection', 'path_depth']:
                    if col not in backfill_df.columns:
                        backfill_df[col] = None
                
                self._save_change_log(backfill_df, change_log_path)

        # Process current sitemap URLs
        processed_urls = []
        for item in sitemap_urls or []:
            if item and isinstance(item, dict) and item.get('loc'):
                processed_urls.append({
                    'loc': item.get('loc'),
                    'lastmod': item.get('lastmod'),
                    'sitemap_source_url': item.get('sitemap_source_url'),
                    'section': item.get('section'),
                    'subsection': item.get('subsection'),
                    'path_depth': item.get('path_depth'),
                })

        current_df = pd.DataFrame(processed_urls)
        if current_df.empty:
            current_df = pd.DataFrame(columns=['loc', 'lastmod', 'sitemap_source_url', 'section'])
        current_df['domain'] = domain

        # Deduplicate
        if not current_df.empty and 'loc' in current_df.columns:
            before = len(current_df)
            if 'lastmod' in current_df.columns:
                # Use utc=True to handle mixed timezone formats consistently
                current_df['lastmod_dt'] = pd.to_datetime(current_df['lastmod'], errors='coerce', utc=True)
                current_df = current_df.sort_values(['loc', 'lastmod_dt'], ascending=[True, False])
                current_df = current_df.drop(columns=['lastmod_dt'])
            current_df = current_df.drop_duplicates(subset=['loc'], keep='first')
            if before > len(current_df):
                logger.info(f"Deduplicated: {before} -> {len(current_df)}")

        # Change detection
        changes = []
        output_rows = []

        if existing_df.empty:
            # First run - all new
            # 🆕 VECTORIZED: Build DataFrames without iterrows()
            logger.info(f"First run: {len(current_df):,} new URLs")
            
            # Filter out null locs
            valid_df = current_df.dropna(subset=['loc']).copy()
            
            if not valid_df.empty:
                # Build changes DataFrame (vectorized)
                changes_df = valid_df.copy()
                changes_df['detected_at'] = current_dt
                changes_df['domain'] = domain
                changes_df['change_type'] = 'discovered'
                changes_df['lastmod_prev'] = None
                
                # Build output DataFrame (vectorized)  
                output_df_new = valid_df.copy()
                output_df_new['detected_at'] = current_dt
                output_df_new['domain'] = domain
                output_df_new['change_type'] = 'discovered'
                
                # Convert to list of dicts for compatibility with existing code
                changes = changes_df.to_dict('records')
                output_rows = output_df_new.to_dict('records')
        else:
            # Merge and compare
            rename_map = {
                'lastmod': 'lastmod_prev',
                'sitemap_source_url': 'sitemap_source_url_prev',
                'change_type': 'change_type_prev',
            }
            
            merge_cols = ['loc']
            for col in ['lastmod', 'sitemap_source_url', 'change_type']:
                if col in existing_df.columns:
                    merge_cols.append(col)
            
            merged = current_df.merge(
                existing_df[list(set(merge_cols))].rename(columns=rename_map),
                on='loc',
                how='outer',
                indicator=True
            )

            for _, row in merged.iterrows():
                loc = row.get('loc')
                if pd.isna(loc):
                    continue

                cur_lastmod = row.get('lastmod')
                cur_source = row.get('sitemap_source_url')
                prev_lastmod = row.get('lastmod_prev')
                prev_source = row.get('sitemap_source_url_prev')
                prev_change_type = row.get('change_type_prev')

                source_for_log = cur_source if pd.notna(cur_source) else prev_source

                # Get first_seen_at from all-time lookup (or current time for new URLs)
                first_seen = all_time_lookup.get(loc, current_dt)
                
                base = {
                    'detected_at': current_dt,
                    'domain': domain,
                    'loc': loc,
                    'first_seen_at': first_seen,
                    'last_seen_at': current_dt,
                    'sitemap_source_url': source_for_log,
                    'section': row.get('section'),
                    'subsection': row.get('subsection'),
                    'path_depth': row.get('path_depth'),
                }

                if row['_merge'] == 'left_only':
                    # New URL - first_seen = last_seen = now
                    base['first_seen_at'] = current_dt
                    changes.append({**base, 'change_type': 'discovered', 'lastmod': cur_lastmod, 'lastmod_prev': None})
                    output_rows.append({**base, 'change_type': 'discovered', 'lastmod': cur_lastmod})

                elif row['_merge'] == 'right_only':
                    # Removed - only log once, last_seen_at = now (when we noticed removal)
                    if prev_change_type != 'removed':
                        changes.append({**base, 'change_type': 'removed', 'lastmod': None, 'lastmod_prev': prev_lastmod})
                    # Don't add to output (removed URLs not in snapshot)

                elif row['_merge'] == 'both':
                    # Check for update
                    updated = cur_lastmod != prev_lastmod and not (pd.isna(cur_lastmod) and pd.isna(prev_lastmod))
                    
                    if updated:
                        changes.append({**base, 'change_type': 'modified', 'lastmod': cur_lastmod, 'lastmod_prev': prev_lastmod})
                        output_rows.append({**base, 'change_type': 'modified', 'lastmod': cur_lastmod})
                    else:
                        output_rows.append({**base, 'change_type': 'present', 'lastmod': cur_lastmod})

        # Build output DataFrame
        output_df = pd.DataFrame(columns=snapshot_columns)
        if output_rows:
            temp_df = pd.DataFrame(output_rows)
            for col in snapshot_columns:
                if col in temp_df.columns:
                    output_df[col] = temp_df[col]

        # Stats
        discovered_count = len([c for c in changes if c.get('change_type') == 'discovered'])
        modified_count = len([c for c in changes if c.get('change_type') == 'modified'])
        removed_count = len([c for c in changes if c.get('change_type') == 'removed'])
        logger.info(f"Changes: {discovered_count} discovered, {modified_count} modified, {removed_count} removed")

        # Section summary
        if not output_df.empty and 'section' in output_df.columns:
            section_counts = output_df['section'].value_counts().head(5)
            logger.info(f"Top sections: {section_counts.to_dict()}")

        # Save change log
        if changes:
            self._save_change_log(pd.DataFrame(changes), change_log_path)

        # Save snapshot
        if not output_df.empty:
            self._save_snapshot(output_df, snapshot_path)
            logger.info(f"Saved snapshot: {len(output_df)} URLs")

        # Update all-time list
        self._update_all_time_live(domain, output_df)

        return output_df
