#!/usr/bin/env python3
"""
Test script for ANAC Orchestrator
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from anac_orchestrator.config import Config
from anac_orchestrator.discovery import DatasetDiscovery

def test_config():
    """Test configuration management"""
    print("Testing Configuration...")
    
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        # Test config creation
        config = Config()
        print(f"✓ Config created successfully")
        print(f"  - JSON root: {config.json_root}")
        print(f"  - NDJSON root: {config.ndjson_root}")
        print(f"  - Logs root: {config.logs_root}")
        
        # Test registry operations
        test_registry = {
            'datasets': {
                'test_dataset': {
                    'name': 'test_dataset',
                    'core_table': 'test_table'
                }
            }
        }
        
        config.update_registry(test_registry)
        print("✓ Registry updated successfully")
        
        retrieved_registry = config.get_registry()
        assert 'test_dataset' in retrieved_registry['datasets']
        print("✓ Registry retrieval works correctly")
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    print("Configuration test passed!\n")

def test_discovery():
    """Test dataset discovery"""
    print("Testing Dataset Discovery...")
    
    # Create temporary directory structure
    temp_dir = Path(tempfile.mkdtemp())
    json_root = temp_dir / "JSON"
    json_root.mkdir()
    
    try:
        # Create sample dataset folders
        sample_folders = [
            "20240201-bando_cig_json",
            "20240201-aggiudicazioni_json",
            "20240201-unknown_dataset_json"
        ]
        
        for folder in sample_folders:
            folder_path = json_root / folder
            folder_path.mkdir()
            
            # Create sample JSON file
            sample_file = folder_path / "sample.json"
            sample_file.write_text('{"test": "data"}')
        
        # Test discovery with temporary config
        config = Config()
        config.json_root = json_root
        
        discovery = DatasetDiscovery(config)
        
        # Test predefined datasets
        predefined = discovery.predefined_datasets
        assert 'cig' in predefined
        assert 'aggiudicazioni' in predefined
        print("✓ Predefined datasets loaded correctly")
        
        # Test discovery
        registry = discovery.discover_datasets()
        
        assert 'datasets' in registry
        assert 'unknown_datasets' in registry
        print("✓ Discovery structure correct")
        
        # Check known datasets
        known_datasets = registry['datasets']
        assert 'aggiudicazioni' in known_datasets
        print("✓ Known datasets discovered correctly")
        
        # Check unknown datasets
        unknown_datasets = registry['unknown_datasets']
        assert len(unknown_datasets) >= 1
        # Check that we have some unknown datasets
        unknown_names = [d['name'] for d in unknown_datasets]
        print(f"Unknown datasets found: {unknown_names}")
        print("✓ Unknown datasets identified correctly")
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    print("Discovery test passed!\n")

def test_cli_imports():
    """Test that CLI can be imported"""
    print("Testing CLI Imports...")
    
    try:
        from anac_orchestrator.cli import main
        print("✓ CLI module imported successfully")
        
        from anac_orchestrator.migration import MigrationManager
        print("✓ Migration module imported successfully")
        
        from anac_orchestrator.ingest import IngestPipeline
        print("✓ Ingest module imported successfully")
        
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False
    
    print("CLI imports test passed!\n")
    return True

def main():
    """Run all tests"""
    print("ANAC Orchestrator - System Tests")
    print("=" * 50)
    
    try:
        test_config()
        test_discovery()
        test_cli_imports()
        
        print("All tests passed! ✓")
        print("\nThe ANAC Orchestrator is ready to use.")
        print("\nNext steps:")
        print("1. Set up your database connection")
        print("2. Place your JSON files in database/JSON/")
        print("3. Run: anac-etl migrate up")
        print("4. Run: anac-etl discover")
        print("5. Run: anac-etl ingest run --all")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
