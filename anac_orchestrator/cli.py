#!/usr/bin/env python3
"""
ANAC Orchestrator CLI
Gestisce migrazioni schema, discovery dataset e pipeline ETL
"""

import argparse
import sys
import logging
from pathlib import Path

from .migration import MigrationManager
from .discovery import DatasetDiscovery
from .ingest import IngestPipeline
from .config import Config

def setup_logging(level=logging.INFO):
    """Setup logging configuration"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('anac_etl.log')
        ]
    )

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='ANAC Orchestrator - Database and ETL Management',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Migration commands
    migrate_parser = subparsers.add_parser('migrate', help='Database migration commands')
    migrate_subparsers = migrate_parser.add_subparsers(dest='migrate_action')
    
    migrate_subparsers.add_parser('up', help='Apply all pending migrations')
    migrate_subparsers.add_parser('status', help='Show migration status')
    
    # Discovery commands
    discover_parser = subparsers.add_parser('discover', help='Discover datasets in JSON folders')
    
    # Registry commands
    registry_parser = subparsers.add_parser('registry', help='Registry management')
    registry_subparsers = registry_parser.add_subparsers(dest='registry_action')
    registry_subparsers.add_parser('export', help='Export registry to YAML config')
    
    # Ingest commands
    ingest_parser = subparsers.add_parser('ingest', help='ETL pipeline commands')
    ingest_subparsers = ingest_parser.add_subparsers(dest='ingest_action')
    
    convert_parser = ingest_subparsers.add_parser('convert', help='Convert JSON to NDJSON')
    convert_parser.add_argument('--dataset', help='Specific dataset to convert')
    convert_parser.add_argument('--since', help='Convert since YYYYMM')
    convert_parser.add_argument('--all', action='store_true', help='Convert all datasets')
    
    load_parser = ingest_subparsers.add_parser('load', help='Load NDJSON to staging')
    load_parser.add_argument('--dataset', help='Specific dataset to load')
    load_parser.add_argument('--since', help='Load since YYYYMM')
    load_parser.add_argument('--all', action='store_true', help='Load all datasets')
    
    upsert_parser = ingest_subparsers.add_parser('upsert', help='Upsert staging to core')
    upsert_parser.add_argument('--dataset', help='Specific dataset to upsert')
    upsert_parser.add_argument('--since', help='Upsert since YYYYMM')
    upsert_parser.add_argument('--all', action='store_true', help='Upsert all datasets')
    
    run_parser = ingest_subparsers.add_parser('run', help='Run full ETL pipeline')
    run_parser.add_argument('--since', help='Run since YYYYMM')
    run_parser.add_argument('--all', action='store_true', help='Run all datasets')
    
    ingest_subparsers.add_parser('status', help='Show ETL status')
    ingest_subparsers.add_parser('dry-run', help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        config = Config()
        
        if args.command == 'migrate':
            migration_manager = MigrationManager(config)
            if args.migrate_action == 'up':
                migration_manager.migrate_up()
            elif args.migrate_action == 'status':
                migration_manager.show_status()
            else:
                migrate_parser.print_help()
                
        elif args.command == 'discover':
            discovery = DatasetDiscovery(config)
            discovery.discover_datasets()
            
        elif args.command == 'registry':
            discovery = DatasetDiscovery(config)
            if args.registry_action == 'export':
                discovery.export_registry()
            else:
                registry_parser.print_help()
                
        elif args.command == 'ingest':
            ingest_pipeline = IngestPipeline(config)
            if args.ingest_action == 'convert':
                ingest_pipeline.convert_json_to_ndjson(
                    dataset=args.dataset,
                    since=args.since,
                    all_datasets=args.all
                )
            elif args.ingest_action == 'load':
                ingest_pipeline.load_ndjson_to_staging(
                    dataset=args.dataset,
                    since=args.since,
                    all_datasets=args.all
                )
            elif args.ingest_action == 'upsert':
                ingest_pipeline.upsert_staging_to_core(
                    dataset=args.dataset,
                    since=args.since,
                    all_datasets=args.all
                )
            elif args.ingest_action == 'run':
                ingest_pipeline.run_full_pipeline(
                    since=args.since,
                    all_datasets=args.all
                )
            elif args.ingest_action == 'status':
                ingest_pipeline.show_status()
            elif args.ingest_action == 'dry-run':
                ingest_pipeline.dry_run()
            else:
                ingest_parser.print_help()
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
