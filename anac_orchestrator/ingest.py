"""
ETL pipeline for ANAC data ingestion
"""

import logging
import json
import hashlib
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import Config
from .discovery import DatasetDiscovery

logger = logging.getLogger(__name__)

class IngestPipeline:
    """Manages ETL pipeline for ANAC data ingestion"""
    
    def __init__(self, config: Config):
        self.config = config
        self.discovery = DatasetDiscovery(config)
        self.connection = None
        self.current_run_id = None
        
    def _get_connection(self):
        """Get database connection"""
        if not self.connection:
            self.connection = pymysql.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                user=self.config.db_user,
                password=self.config.db_password,
                database=self.config.db_name,
                charset='utf8mb4',
                autocommit=False
            )
        return self.connection
    
    def _close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def _execute_sql(self, sql: str, params: tuple = None) -> Any:
        """Execute SQL statement"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            result = cursor.fetchall()
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            logger.error(f"SQL execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def _start_etl_run(self, notes: str = "") -> int:
        """Start a new ETL run and return run_id"""
        sql = """
        INSERT INTO etl_runs (started_at, status, notes)
        VALUES (NOW(), 'RUNNING', %s)
        """
        self._execute_sql(sql, (notes,))
        
        # Get the run_id
        result = self._execute_sql("SELECT LAST_INSERT_ID()")
        run_id = result[0][0]
        self.current_run_id = run_id
        
        logger.info(f"Started ETL run {run_id}")
        return run_id
    
    def _end_etl_run(self, status: str, total_files: int = 0, total_rows: int = 0, total_errors: int = 0):
        """End current ETL run"""
        if not self.current_run_id:
            return
        
        sql = """
        UPDATE etl_runs 
        SET ended_at = NOW(), status = %s, total_files = %s, total_rows = %s, total_errors = %s
        WHERE run_id = %s
        """
        self._execute_sql(sql, (status, total_files, total_rows, total_errors, self.current_run_id))
        
        logger.info(f"Ended ETL run {self.current_run_id} with status {status}")
        self.current_run_id = None
    
    def _record_etl_file(self, dataset: str, file_path: str, md5: str, rows_loaded: int, status: str, error_msg: str = None) -> int:
        """Record ETL file processing"""
        sql = """
        INSERT INTO etl_files (run_id, dataset, path, md5, rows_loaded, status, error_msg, started_at, ended_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        self._execute_sql(sql, (self.current_run_id, dataset, file_path, md5, rows_loaded, status, error_msg))
        
        result = self._execute_sql("SELECT LAST_INSERT_ID()")
        return result[0][0]
    
    def _calculate_file_md5(self, file_path: Path) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def _find_json_files(self, dataset: str, since: str = None) -> List[Path]:
        """Find JSON files for a dataset"""
        dataset_config = self.discovery.get_dataset_config(dataset)
        if not dataset_config:
            logger.warning(f"Dataset {dataset} not found in registry")
            return []
        
        json_files = []
        pattern = dataset_config['folder_glob']
        
        # Find matching folders
        for folder in self.config.json_root.glob(f"*-{dataset}_json"):
            if since and folder.name < since:
                continue
            
            # Find JSON files in folder
            for json_file in folder.glob("*.json"):
                json_files.append(json_file)
        
        return sorted(json_files)
    
    def convert_json_to_ndjson(self, dataset: str = None, since: str = None, all_datasets: bool = False):
        """Convert JSON files to NDJSON format"""
        logger.info("Starting JSON to NDJSON conversion")
        
        if all_datasets:
            datasets = self.discovery.list_datasets()
        elif dataset:
            datasets = [dataset]
        else:
            datasets = self.discovery.list_datasets()
        
        total_files = 0
        total_errors = 0
        
        for dataset_name in datasets:
            logger.info(f"Converting dataset: {dataset_name}")
            
            json_files = self._find_json_files(dataset_name, since)
            if not json_files:
                logger.warning(f"No JSON files found for dataset {dataset_name}")
                continue
            
            dataset_config = self.discovery.get_dataset_config(dataset_name)
            json_pointer = dataset_config.get('json_pointer', 'item')
            
            for json_file in json_files:
                try:
                    # Calculate relative path for NDJSON output
                    rel_path = json_file.relative_to(self.config.json_root)
                    ndjson_path = self.config.ndjson_root / rel_path.with_suffix('.ndjson')
                    
                    # Create output directory
                    ndjson_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Convert JSON to NDJSON
                    self._convert_single_json_file(json_file, ndjson_path, json_pointer)
                    
                    total_files += 1
                    logger.info(f"Converted: {json_file} -> {ndjson_path}")
                    
                except Exception as e:
                    total_errors += 1
                    logger.error(f"Error converting {json_file}: {e}")
        
        logger.info(f"Conversion completed. Files: {total_files}, Errors: {total_errors}")
    
    def _convert_single_json_file(self, json_file: Path, ndjson_path: Path, json_pointer: str):
        """Convert a single JSON file to NDJSON"""
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract items using json_pointer
        if json_pointer == 'item' and isinstance(data, list):
            items = data
        elif '.' in json_pointer:
            # Handle nested pointers like "records.item"
            parts = json_pointer.split('.')
            items = data
            for part in parts:
                items = items.get(part, [])
        else:
            items = data.get(json_pointer, [])
        
        if not isinstance(items, list):
            items = [items]
        
        # Write NDJSON
        with open(ndjson_path, 'w', encoding='utf-8') as f:
            for item in items:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    def load_ndjson_to_staging(self, dataset: str = None, since: str = None, all_datasets: bool = False):
        """Load NDJSON files to staging tables"""
        logger.info("Starting NDJSON to staging load")
        
        run_id = self._start_etl_run("NDJSON to staging load")
        
        try:
            if all_datasets:
                datasets = self.discovery.list_datasets()
            elif dataset:
                datasets = [dataset]
            else:
                datasets = self.discovery.list_datasets()
            
            total_files = 0
            total_rows = 0
            total_errors = 0
            
            for dataset_name in datasets:
                logger.info(f"Loading dataset: {dataset_name}")
                
                # Create staging JSON table if not exists
                self._create_staging_json_table(dataset_name)
                
                # Find NDJSON files
                ndjson_files = self._find_ndjson_files(dataset_name, since)
                if not ndjson_files:
                    logger.warning(f"No NDJSON files found for dataset {dataset_name}")
                    continue
                
                for ndjson_file in ndjson_files:
                    try:
                        rows_loaded = self._load_single_ndjson_file(dataset_name, ndjson_file)
                        
                        md5 = self._calculate_file_md5(ndjson_file)
                        self._record_etl_file(dataset_name, str(ndjson_file), md5, rows_loaded, 'OK')
                        
                        total_files += 1
                        total_rows += rows_loaded
                        
                        logger.info(f"Loaded {rows_loaded} rows from {ndjson_file}")
                        
                    except Exception as e:
                        total_errors += 1
                        md5 = self._calculate_file_md5(ndjson_file)
                        self._record_etl_file(dataset_name, str(ndjson_file), md5, 0, 'ERROR', str(e))
                        logger.error(f"Error loading {ndjson_file}: {e}")
            
            self._end_etl_run('OK' if total_errors == 0 else 'PARTIAL', total_files, total_rows, total_errors)
            
        except Exception as e:
            self._end_etl_run('ERROR', 0, 0, 1)
            logger.error(f"Load process failed: {e}")
            raise
        finally:
            self._close_connection()
    
    def _find_ndjson_files(self, dataset: str, since: str = None) -> List[Path]:
        """Find NDJSON files for a dataset"""
        ndjson_files = []
        
        # Find matching folders
        for folder in self.config.ndjson_root.glob(f"*-{dataset}_json"):
            if since and folder.name < since:
                continue
            
            # Find NDJSON files in folder
            for ndjson_file in folder.glob("*.ndjson"):
                ndjson_files.append(ndjson_file)
        
        return sorted(ndjson_files)
    
    def _create_staging_json_table(self, dataset: str):
        """Create staging JSON table for dataset"""
        table_name = f"stg_{dataset}_json"
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            payload JSON,
            _file_name VARCHAR(255),
            _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_file_name (_file_name),
            INDEX idx_ingested_at (_ingested_at)
        )
        """
        self._execute_sql(sql)
    
    def _load_single_ndjson_file(self, dataset: str, ndjson_file: Path) -> int:
        """Load a single NDJSON file to staging"""
        table_name = f"stg_{dataset}_json"
        file_name = ndjson_file.name
        
        # Count existing rows for this file
        count_sql = f"SELECT COUNT(*) FROM {table_name} WHERE _file_name = %s"
        result = self._execute_sql(count_sql, (file_name,))
        existing_rows = result[0][0]
        
        if existing_rows > 0:
            logger.info(f"File {file_name} already loaded ({existing_rows} rows), skipping")
            return existing_rows
        
        # Load data using LOAD DATA LOCAL INFILE
        sql = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {table_name}
        FIELDS TERMINATED BY '\n'
        LINES TERMINATED BY '\n'
        (payload)
        SET _file_name = %s
        """
        
        self._execute_sql(sql, (str(ndjson_file), file_name))
        
        # Count loaded rows
        result = self._execute_sql(count_sql, (file_name,))
        return result[0][0]
    
    def upsert_staging_to_core(self, dataset: str = None, since: str = None, all_datasets: bool = False):
        """Upsert data from staging to core tables"""
        logger.info("Starting staging to core upsert")
        
        run_id = self._start_etl_run("Staging to core upsert")
        
        try:
            if all_datasets:
                datasets = self.discovery.list_datasets()
            elif dataset:
                datasets = [dataset]
            else:
                datasets = self.discovery.list_datasets()
            
            # Sort datasets by dependencies (parents first)
            sorted_datasets = self._sort_datasets_by_dependencies(datasets)
            
            total_rows = 0
            total_errors = 0
            
            for dataset_name in sorted_datasets:
                logger.info(f"Upserting dataset: {dataset_name}")
                
                try:
                    rows_processed = self._upsert_single_dataset(dataset_name)
                    total_rows += rows_processed
                    logger.info(f"Upserted {rows_processed} rows for {dataset_name}")
                    
                except Exception as e:
                    total_errors += 1
                    logger.error(f"Error upserting {dataset_name}: {e}")
            
            self._end_etl_run('OK' if total_errors == 0 else 'PARTIAL', 0, total_rows, total_errors)
            
        except Exception as e:
            self._end_etl_run('ERROR', 0, 0, 1)
            logger.error(f"Upsert process failed: {e}")
            raise
        finally:
            self._close_connection()
    
    def _sort_datasets_by_dependencies(self, datasets: List[str]) -> List[str]:
        """Sort datasets by dependencies (parents first)"""
        registry = self.config.get_registry()
        dataset_configs = registry.get('datasets', {})
        
        sorted_datasets = []
        remaining = set(datasets)
        
        while remaining:
            # Find datasets with no unresolved dependencies
            ready = []
            for dataset in remaining:
                config = dataset_configs.get(dataset, {})
                depends_on = config.get('depends_on', [])
                
                # Check if all dependencies are already processed or not in our list
                unresolved_deps = [dep for dep in depends_on if dep in remaining]
                if not unresolved_deps:
                    ready.append(dataset)
            
            if not ready:
                # Circular dependency or missing dependency
                logger.warning(f"Circular dependency detected or missing dependency. Remaining: {remaining}")
                sorted_datasets.extend(remaining)
                break
            
            # Add ready datasets to sorted list
            for dataset in ready:
                sorted_datasets.append(dataset)
                remaining.remove(dataset)
        
        return sorted_datasets
    
    def _upsert_single_dataset(self, dataset: str) -> int:
        """Upsert a single dataset from staging to core"""
        dataset_config = self.discovery.get_dataset_config(dataset)
        if not dataset_config:
            raise ValueError(f"Dataset {dataset} not found in registry")
        
        # Create staging table if not exists
        self._create_staging_table(dataset)
        
        # Project from JSON to staging table
        self._project_json_to_staging(dataset)
        
        # Upsert from staging to core
        rows_upserted = self._upsert_staging_to_core_table(dataset)
        
        return rows_upserted
    
    def _create_staging_table(self, dataset: str):
        """Create staging table for dataset"""
        dataset_config = self.discovery.get_dataset_config(dataset)
        table_name = f"stg_{dataset}"
        
        # Build column definitions from select_map
        columns = []
        for column, _ in dataset_config.get('select_map', {}).items():
            columns.append(f"{column} TEXT")
        
        if columns:
            columns_sql = ", ".join(columns)
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
            self._execute_sql(sql)
    
    def _project_json_to_staging(self, dataset: str):
        """Project data from JSON staging to typed staging table"""
        dataset_config = self.discovery.get_dataset_config(dataset)
        json_table = f"stg_{dataset}_json"
        staging_table = f"stg_{dataset}"
        
        select_map = dataset_config.get('select_map', {})
        if not select_map:
            logger.warning(f"No select_map defined for {dataset}")
            return
        
        # Build SELECT clause
        select_clauses = []
        for column, expression in select_map.items():
            select_clauses.append(f"{expression} AS {column}")
        
        select_sql = ", ".join(select_clauses)
        
        # Clear staging table
        self._execute_sql(f"TRUNCATE TABLE {staging_table}")
        
        # Insert projected data
        sql = f"""
        INSERT INTO {staging_table} ({', '.join(select_map.keys())})
        SELECT {select_sql}
        FROM {json_table}
        """
        self._execute_sql(sql)
    
    def _upsert_staging_to_core_table(self, dataset: str) -> int:
        """Upsert data from staging to core table"""
        dataset_config = self.discovery.get_dataset_config(dataset)
        staging_table = f"stg_{dataset}"
        core_table = dataset_config['core_table']
        key_field = dataset_config['key']
        update_fields = dataset_config.get('upsert_update_fields', [])
        
        # Build UPDATE clause
        if update_fields:
            update_clauses = [f"{field} = VALUES({field})" for field in update_fields]
            update_sql = ", ".join(update_clauses)
        else:
            update_sql = f"{key_field} = VALUES({key_field})"
        
        # Count rows before upsert
        result = self._execute_sql(f"SELECT COUNT(*) FROM {staging_table}")
        staging_count = result[0][0]
        
        if staging_count == 0:
            return 0
        
        # Perform upsert
        sql = f"""
        INSERT INTO {core_table} ({', '.join(dataset_config['select_map'].keys())})
        SELECT {', '.join(dataset_config['select_map'].keys())}
        FROM {staging_table}
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        
        self._execute_sql(sql)
        
        return staging_count
    
    def run_full_pipeline(self, since: str = None, all_datasets: bool = False):
        """Run the complete ETL pipeline"""
        logger.info("Starting full ETL pipeline")
        
        try:
            # Step 1: Convert JSON to NDJSON
            self.convert_json_to_ndjson(since=since, all_datasets=all_datasets)
            
            # Step 2: Load NDJSON to staging
            self.load_ndjson_to_staging(since=since, all_datasets=all_datasets)
            
            # Step 3: Upsert staging to core
            self.upsert_staging_to_core(since=since, all_datasets=all_datasets)
            
            logger.info("Full ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Full ETL pipeline failed: {e}")
            raise
    
    def show_status(self):
        """Show ETL status and statistics"""
        try:
            # Show last run
            result = self._execute_sql("""
                SELECT run_id, started_at, ended_at, status, total_files, total_rows, total_errors, notes
                FROM etl_runs 
                ORDER BY run_id DESC 
                LIMIT 1
            """)
            
            if result:
                run = result[0]
                print(f"Last ETL Run:")
                print(f"  Run ID: {run[0]}")
                print(f"  Started: {run[1]}")
                print(f"  Ended: {run[2]}")
                print(f"  Status: {run[3]}")
                print(f"  Files: {run[4]}")
                print(f"  Rows: {run[5]}")
                print(f"  Errors: {run[6]}")
                print(f"  Notes: {run[7]}")
            else:
                print("No ETL runs found")
            
            # Show pending children
            print(f"\nPending Children (records in staging without parent):")
            datasets = self.discovery.list_datasets()
            
            for dataset in datasets:
                dataset_config = self.discovery.get_dataset_config(dataset)
                if not dataset_config:
                    continue
                
                depends_on = dataset_config.get('depends_on', [])
                if depends_on:
                    staging_table = f"stg_{dataset}"
                    core_table = dataset_config['core_table']
                    key_field = dataset_config['key']
                    
                    # Check for pending records
                    for dep in depends_on:
                        dep_config = self.discovery.get_dataset_config(dep)
                        if dep_config:
                            dep_table = dep_config['core_table']
                            dep_key = dep_config['key']
                            
                            # This is a simplified check - in reality you'd need to check FK relationships
                            result = self._execute_sql(f"""
                                SELECT COUNT(*) FROM {staging_table} s
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM {dep_table} c 
                                    WHERE c.{dep_key} = s.{key_field}
                                )
                            """)
                            
                            pending_count = result[0][0]
                            if pending_count > 0:
                                print(f"  {dataset}: {pending_count} records pending {dep}")
            
        except Exception as e:
            logger.error(f"Error showing status: {e}")
        finally:
            self._close_connection()
    
    def dry_run(self):
        """Show what would be done without executing"""
        print("DRY RUN - No actual changes will be made")
        print("=" * 50)
        
        datasets = self.discovery.list_datasets()
        print(f"Configured datasets: {len(datasets)}")
        
        for dataset in datasets:
            dataset_config = self.discovery.get_dataset_config(dataset)
            if dataset_config:
                print(f"\nDataset: {dataset}")
                print(f"  Core table: {dataset_config['core_table']}")
                print(f"  Staging table: {dataset_config['stg_table']}")
                print(f"  Key field: {dataset_config['key']}")
                print(f"  Dependencies: {dataset_config.get('depends_on', [])}")
                
                # Count available files
                json_files = self._find_json_files(dataset)
                print(f"  JSON files available: {len(json_files)}")
                
                if json_files:
                    print(f"  Sample files:")
                    for f in json_files[:3]:
                        print(f"    - {f}")
                    if len(json_files) > 3:
                        print(f"    ... and {len(json_files) - 3} more")
        
        print(f"\nProcessing order (by dependencies):")
        sorted_datasets = self._sort_datasets_by_dependencies(datasets)
        for i, dataset in enumerate(sorted_datasets, 1):
            print(f"  {i}. {dataset}")
