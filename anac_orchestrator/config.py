"""
Configuration management for ANAC Orchestrator
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

class Config:
    """Configuration manager for ANAC Orchestrator"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or os.getenv('ANAC_CONFIG_PATH', 'config/anac_etl.yml')
        self.config = self._load_config()
        
        # Database configuration
        self.db_host = os.getenv('ANAC_DB_HOST', 'localhost')
        self.db_port = int(os.getenv('ANAC_DB_PORT', '3306'))
        self.db_name = os.getenv('ANAC_DB_NAME', 'anacdb')
        self.db_user = os.getenv('ANAC_DB_USER', 'root')
        self.db_password = os.getenv('ANAC_DB_PASSWORD', '')
        
        # Paths
        self.json_root = Path(os.getenv('ANAC_JSON_ROOT', 'database/JSON'))
        self.ndjson_root = Path(os.getenv('ANAC_NDJSON_ROOT', 'database/NDJSON'))
        self.logs_root = Path(os.getenv('ANAC_LOGS_ROOT', 'database/logs'))
        self.script_path = Path(os.getenv('ANAC_SCRIPT_PATH', 'Script_creazioneDB_Anac.txt'))
        
        # Ensure directories exist
        self.json_root.mkdir(parents=True, exist_ok=True)
        self.ndjson_root.mkdir(parents=True, exist_ok=True)
        self.logs_root.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    def save_config(self):
        """Save current configuration to YAML file"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
    
    def get_database_url(self) -> str:
        """Get database connection URL"""
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    def get_registry(self) -> Dict[str, Any]:
        """Get dataset registry from config"""
        return self.config.get('registry', {})
    
    def update_registry(self, registry: Dict[str, Any]):
        """Update dataset registry in config"""
        self.config['registry'] = registry
        self.save_config()
