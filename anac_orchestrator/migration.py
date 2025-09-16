"""
Database migration management for ANAC Orchestrator
"""

import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
import pymysql
from datetime import datetime

from .config import Config

logger = logging.getLogger(__name__)

class MigrationManager:
    """Manages database migrations for ANAC schema"""
    
    def __init__(self, config: Config):
        self.config = config
        self.connection = None
        
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
    
    def _get_schema_version(self) -> int:
        """Get current schema version"""
        try:
            result = self._execute_sql("SELECT MAX(version) FROM schema_version")
            return result[0][0] if result and result[0][0] else 0
        except pymysql.Error:
            # Table doesn't exist yet
            return 0
    
    def _create_schema_version_table(self):
        """Create schema_version table if it doesn't exist"""
        sql = """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INT PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum VARCHAR(64),
            notes TEXT
        )
        """
        self._execute_sql(sql)
        logger.info("Created schema_version table")
    
    def _record_migration(self, version: int, checksum: str, notes: str):
        """Record migration in schema_version table"""
        sql = """
        INSERT INTO schema_version (version, checksum, notes)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
        applied_at = CURRENT_TIMESTAMP,
        checksum = VALUES(checksum),
        notes = VALUES(notes)
        """
        self._execute_sql(sql, (version, checksum, notes))
    
    def _calculate_checksum(self, content: str) -> str:
        """Calculate SHA256 checksum of content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def migrate_v1(self):
        """Apply migration v1: Execute original TXT script"""
        logger.info("Starting migration v1: Original schema creation")
        
        if not self.config.script_path.exists():
            raise FileNotFoundError(f"Script file not found: {self.config.script_path}")
        
        with open(self.config.script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        checksum = self._calculate_checksum(script_content)
        
        try:
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    self._execute_sql(statement)
            
            self._record_migration(1, checksum, "Original schema creation from TXT")
            logger.info("Migration v1 completed successfully")
            
        except Exception as e:
            logger.error(f"Migration v1 failed: {e}")
            raise
    
    def migrate_v2(self):
        """Apply migration v2: Add FK, ETL tables, and indexes"""
        logger.info("Starting migration v2: FK, ETL tables, and indexes")
        
        try:
            # Create ETL tables
            self._create_etl_tables()
            
            # Add foreign keys
            self._add_foreign_keys()
            
            # Add indexes
            self._add_indexes()
            
            checksum = self._calculate_checksum("migration_v2_fk_etl_indexes")
            self._record_migration(2, checksum, "Added FK, ETL tables, and indexes")
            logger.info("Migration v2 completed successfully")
            
        except Exception as e:
            logger.error(f"Migration v2 failed: {e}")
            raise
    
    def _create_etl_tables(self):
        """Create ETL service tables"""
        etl_tables = {
            'etl_runs': """
                CREATE TABLE IF NOT EXISTS etl_runs (
                    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    started_at DATETIME,
                    ended_at DATETIME,
                    status ENUM('OK','ERROR','PARTIAL'),
                    total_files INT,
                    total_rows BIGINT,
                    total_errors INT,
                    notes TEXT,
                    git_hash VARCHAR(40)
                )
            """,
            'etl_files': """
                CREATE TABLE IF NOT EXISTS etl_files (
                    file_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    run_id BIGINT,
                    dataset VARCHAR(64),
                    path TEXT,
                    md5 CHAR(32),
                    rows_loaded BIGINT,
                    status ENUM('OK','ERROR'),
                    error_msg TEXT,
                    started_at DATETIME,
                    ended_at DATETIME,
                    KEY(run_id),
                    KEY(dataset)
                )
            """,
            'etl_rejects': """
                CREATE TABLE IF NOT EXISTS etl_rejects (
                    run_id BIGINT,
                    dataset VARCHAR(64),
                    reason VARCHAR(128),
                    payload_json JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    KEY(run_id),
                    KEY(dataset)
                )
            """
        }
        
        for table_name, sql in etl_tables.items():
            self._execute_sql(sql)
            logger.info(f"Created ETL table: {table_name}")
    
    def _add_foreign_keys(self):
        """Add foreign keys to bando_cig"""
        fk_definitions = [
            ("cup", "fk_cup_cig"),
            ("stazioni_appaltanti", "fk_stazioni_appaltanti_cig"),
            ("categorie_opera", "fk_categorie_opera_cig"),
            ("categorie_dpcm_aggregazione", "fk_categorie_dpcm_aggregazione_cig"),
            ("lavorazioni", "fk_lavorazioni_cig"),
            ("partecipanti", "fk_partecipanti_cig"),
            ("aggiudicazioni", "fk_aggiudicazioni_cig")
        ]
        
        for table, constraint_name in fk_definitions:
            try:
                # Check if table exists and has cig column
                result = self._execute_sql(f"""
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = DATABASE() 
                    AND table_name = '{table}' 
                    AND column_name = 'cig'
                """)
                
                if result[0][0] > 0:
                    # Check if FK already exists
                    fk_check = self._execute_sql(f"""
                        SELECT COUNT(*) FROM information_schema.key_column_usage 
                        WHERE table_schema = DATABASE() 
                        AND table_name = '{table}' 
                        AND constraint_name = '{constraint_name}'
                    """)
                    
                    if fk_check[0][0] == 0:
                        sql = f"""
                            ALTER TABLE {table} 
                            ADD CONSTRAINT {constraint_name}
                            FOREIGN KEY (cig) REFERENCES bando_cig(cig)
                        """
                        self._execute_sql(sql)
                        logger.info(f"Added FK: {constraint_name}")
                    else:
                        logger.info(f"FK already exists: {constraint_name}")
                else:
                    logger.warning(f"Table {table} does not have cig column, skipping FK")
                    
            except Exception as e:
                logger.warning(f"Could not add FK {constraint_name}: {e}")
    
    def _add_indexes(self):
        """Add support indexes"""
        indexes = [
            ("bando_cig", "cf_amministrazione_appaltante"),
            ("aggiudicazioni", "cig"),
            ("aggiudicazioni", "id_aggiudicazione"),
            ("partecipanti", "cig"),
            ("aggiudicatari", "id_aggiudicazioni"),
            ("subappalti", "id_aggiudicazione"),
            ("stati_avanzamento", "id_aggiudicazione"),
            ("varianti", "id_aggiudicazione"),
            ("fine_contratto", "id_aggiudicazione"),
            ("collaudo", "id_aggiudicazione"),
            ("quadro_economico", "id_aggiudicazione"),
            ("fonti_finanziamento", "id_aggiudicazione")
        ]
        
        for table, column in indexes:
            try:
                index_name = f"idx_{table}_{column}"
                
                # Check if index already exists
                result = self._execute_sql(f"""
                    SELECT COUNT(*) FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() 
                    AND table_name = '{table}' 
                    AND index_name = '{index_name}'
                """)
                
                if result[0][0] == 0:
                    # Check if table and column exist
                    col_check = self._execute_sql(f"""
                        SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_schema = DATABASE() 
                        AND table_name = '{table}' 
                        AND column_name = '{column}'
                    """)
                    
                    if col_check[0][0] > 0:
                        sql = f"CREATE INDEX {index_name} ON {table} ({column})"
                        self._execute_sql(sql)
                        logger.info(f"Created index: {index_name}")
                    else:
                        logger.warning(f"Column {table}.{column} does not exist, skipping index")
                else:
                    logger.info(f"Index already exists: {index_name}")
                    
            except Exception as e:
                logger.warning(f"Could not create index {table}.{column}: {e}")
    
    def migrate_up(self):
        """Apply all pending migrations"""
        logger.info("Starting migration process")
        
        try:
            self._create_schema_version_table()
            current_version = self._get_schema_version()
            
            if current_version < 1:
                self.migrate_v1()
                current_version = 1
            
            if current_version < 2:
                self.migrate_v2()
                current_version = 2
            
            logger.info(f"Migration completed. Current version: {current_version}")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            self._close_connection()
    
    def show_status(self):
        """Show current migration status"""
        try:
            self._create_schema_version_table()
            current_version = self._get_schema_version()
            
            print(f"Current schema version: {current_version}")
            
            # Show migration history
            result = self._execute_sql("SELECT * FROM schema_version ORDER BY version")
            if result:
                print("\nMigration history:")
                print("Version | Applied At | Checksum | Notes")
                print("-" * 80)
                for row in result:
                    print(f"{row[0]:7} | {row[1]} | {row[2][:16]}... | {row[3]}")
            else:
                print("No migrations applied yet")
                
        except Exception as e:
            logger.error(f"Error showing status: {e}")
        finally:
            self._close_connection()
