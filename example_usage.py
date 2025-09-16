#!/usr/bin/env python3
"""
Example usage of ANAC Orchestrator
"""

import os
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from anac_orchestrator.config import Config
from anac_orchestrator.migration import MigrationManager
from anac_orchestrator.discovery import DatasetDiscovery
from anac_orchestrator.ingest import IngestPipeline

def main():
    """Example usage of ANAC Orchestrator"""
    
    print("ANAC Orchestrator - Example Usage")
    print("=" * 50)
    
    # Initialize configuration
    config = Config()
    print(f"Configuration loaded from: {config.config_path}")
    print(f"Database: {config.db_host}:{config.db_port}/{config.db_name}")
    print(f"JSON root: {config.json_root}")
    print(f"NDJSON root: {config.ndjson_root}")
    
    # Example 1: Database Migration
    print("\n1. Database Migration Example")
    print("-" * 30)
    
    try:
        migration_manager = MigrationManager(config)
        print("Migration manager initialized")
        
        # Show current status
        print("Current migration status:")
        migration_manager.show_status()
        
        # Uncomment to run migrations (requires database connection)
        # migration_manager.migrate_up()
        
    except Exception as e:
        print(f"Migration example failed: {e}")
    
    # Example 2: Dataset Discovery
    print("\n2. Dataset Discovery Example")
    print("-" * 30)
    
    try:
        discovery = DatasetDiscovery(config)
        print("Discovery manager initialized")
        
        # Show predefined datasets
        print("Predefined datasets:")
        for name, config_data in discovery.predefined_datasets.items():
            print(f"  - {name}: {config_data['core_table']}")
        
        # Uncomment to run discovery (requires JSON files)
        # registry = discovery.discover_datasets()
        # print(f"Discovery completed. Found {len(registry.get('datasets', {}))} datasets")
        
    except Exception as e:
        print(f"Discovery example failed: {e}")
    
    # Example 3: ETL Pipeline
    print("\n3. ETL Pipeline Example")
    print("-" * 30)
    
    try:
        ingest_pipeline = IngestPipeline(config)
        print("Ingest pipeline initialized")
        
        # Show dry run
        print("Dry run simulation:")
        ingest_pipeline.dry_run()
        
        # Uncomment to run actual ETL (requires database and files)
        # ingest_pipeline.run_full_pipeline(all_datasets=True)
        
    except Exception as e:
        print(f"ETL pipeline example failed: {e}")
    
    print("\nExample completed!")
    print("\nTo run actual operations:")
    print("1. Set up database connection (environment variables)")
    print("2. Place JSON files in database/JSON/")
    print("3. Run: anac-etl migrate up")
    print("4. Run: anac-etl discover")
    print("5. Run: anac-etl ingest run --all")

if __name__ == "__main__":
    main()
