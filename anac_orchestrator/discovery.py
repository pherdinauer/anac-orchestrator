"""
Dataset discovery and registry management for ANAC Orchestrator
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml

from .config import Config

logger = logging.getLogger(__name__)

class DatasetDiscovery:
    """Manages dataset discovery and registry generation"""
    
    def __init__(self, config: Config):
        self.config = config
        
        # Predefined dataset mappings based on the real ANAC structure
        self.predefined_datasets = {
            'cig': {
                'name': 'cig',
                'folder_glob': '*-cig_json',
                'json_pointer': 'item',
                'core_table': 'bando_cig',
                'stg_json_table': 'stg_cig_json',
                'stg_table': 'stg_cig',
                'key': 'cig',
                'depends_on': [],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'cf_amministrazione_appaltante': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cf_amministrazione_appaltante"))',
                    'denominazione_amministrazione_appaltante': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.denominazione_amministrazione_appaltante"))',
                    'oggetto_gara': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.oggetto_gara"))',
                    'importo_complessivo_gara': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.importo_complessivo_gara")) AS DOUBLE)',
                    'data_pubblicazione': 'STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.data_pubblicazione")), "%Y-%m-%d")',
                    'data_scadenza_offerta': 'STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.data_scadenza_offerta")), "%Y-%m-%d")'
                },
                'upsert_update_fields': ['cf_amministrazione_appaltante', 'denominazione_amministrazione_appaltante', 'oggetto_gara', 'importo_complessivo_gara', 'data_pubblicazione', 'data_scadenza_offerta']
            },
            'aggiudicazioni': {
                'name': 'aggiudicazioni',
                'folder_glob': '*-aggiudicazioni_json',
                'json_pointer': 'item',
                'core_table': 'aggiudicazioni',
                'stg_json_table': 'stg_aggiudicazioni_json',
                'stg_table': 'stg_aggiudicazioni',
                'key': 'id_aggiudicazione',
                'depends_on': ['cig'],
                'select_map': {
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'importo_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.importo_aggiudicazione")) AS DOUBLE)',
                    'data_aggiudicazione_definitiva': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.data_aggiudicazione_definitiva"))',
                    'esito': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.esito"))'
                },
                'upsert_update_fields': ['importo_aggiudicazione', 'data_aggiudicazione_definitiva', 'esito']
            },
            'aggiudicatari': {
                'name': 'aggiudicatari',
                'folder_glob': '*-aggiudicatari_json',
                'json_pointer': 'item',
                'core_table': 'aggiudicatari',
                'stg_json_table': 'stg_aggiudicatari_json',
                'stg_table': 'stg_aggiudicatari',
                'key': 'codice_fiscale',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'codice_fiscale': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.codice_fiscale"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'ruolo': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.ruolo"))',
                    'denominazione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.denominazione"))',
                    'tipo_soggetto': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.tipo_soggetto"))',
                    'id_aggiudicazioni': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazioni")) AS INT)'
                },
                'upsert_update_fields': ['ruolo', 'denominazione', 'tipo_soggetto', 'id_aggiudicazioni']
            },
            'partecipanti': {
                'name': 'partecipanti',
                'folder_glob': '*-partecipanti_json',
                'json_pointer': 'item',
                'core_table': 'partecipanti',
                'stg_json_table': 'stg_partecipanti_json',
                'stg_table': 'stg_partecipanti',
                'key': 'codice_fiscale',
                'depends_on': ['cig'],
                'select_map': {
                    'codice_fiscale': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.codice_fiscale"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'ruolo': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.ruolo"))',
                    'denominazione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.denominazione"))',
                    'tipo_soggetto': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.tipo_soggetto"))'
                },
                'upsert_update_fields': ['ruolo', 'denominazione', 'tipo_soggetto']
            },
            'cup': {
                'name': 'cup',
                'folder_glob': '*-cup_json',
                'json_pointer': 'item',
                'core_table': 'cup',
                'stg_json_table': 'stg_cup_json',
                'stg_table': 'stg_cup',
                'key': 'cup',
                'depends_on': ['cig'],
                'select_map': {
                    'cup': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cup"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))'
                },
                'upsert_update_fields': ['cig']
            },
            'stazioni_appaltanti': {
                'name': 'stazioni_appaltanti',
                'folder_glob': '*-stazioni-appaltanti_json',
                'json_pointer': 'item',
                'core_table': 'stazioni_appaltanti',
                'stg_json_table': 'stg_stazioni_appaltanti_json',
                'stg_table': 'stg_stazioni_appaltanti',
                'key': 'codice_fiscale',
                'depends_on': ['cig'],
                'select_map': {
                    'codice_fiscale': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.codice_fiscale"))',
                    'denominazione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.denominazione"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))'
                },
                'upsert_update_fields': ['denominazione', 'cig']
            },
            'categorie_opera': {
                'name': 'categorie_opera',
                'folder_glob': '*-categorie-opera_json',
                'json_pointer': 'item',
                'core_table': 'categorie_opera',
                'stg_json_table': 'stg_categorie_opera_json',
                'stg_table': 'stg_categorie_opera',
                'key': 'id',
                'depends_on': ['cig'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_categoria': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_categoria"))',
                    'descrizione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.descrizione"))',
                    'cod_tipo_categoria': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cod_tipo_categoria"))'
                },
                'upsert_update_fields': ['id_categoria', 'descrizione', 'cod_tipo_categoria']
            },
            'categorie_dpcm_aggregazione': {
                'name': 'categorie_dpcm_aggregazione',
                'folder_glob': '*-categorie-dpcm-aggregazione_json',
                'json_pointer': 'item',
                'core_table': 'categorie_dpcm_aggregazione',
                'stg_json_table': 'stg_categorie_dpcm_aggregazione_json',
                'stg_table': 'stg_categorie_dpcm_aggregazione',
                'key': 'id',
                'depends_on': ['cig'],
                'select_map': {
                    'id_categoria': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_categoria"))',
                    'descrizione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.descrizione"))',
                    'cod_tipo_categoria': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cod_tipo_categoria"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))'
                },
                'upsert_update_fields': ['id_categoria', 'descrizione', 'cod_tipo_categoria', 'cig']
            },
            'lavorazioni': {
                'name': 'lavorazioni',
                'folder_glob': '*-lavorazioni_json',
                'json_pointer': 'item',
                'core_table': 'lavorazioni',
                'stg_json_table': 'stg_lavorazioni_json',
                'stg_table': 'stg_lavorazioni',
                'key': 'cod_tipo_lavorazione',
                'depends_on': ['cig'],
                'select_map': {
                    'cod_tipo_lavorazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cod_tipo_lavorazione")) AS INT)',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'tipo_lavorazione': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.tipo_lavorazione"))'
                },
                'upsert_update_fields': ['cig', 'tipo_lavorazione']
            },
            'subappalti': {
                'name': 'subappalti',
                'folder_glob': '*-subappalti_json',
                'json_pointer': 'item',
                'core_table': 'subappalti',
                'stg_json_table': 'stg_subappalti_json',
                'stg_table': 'stg_subappalti',
                'key': 'id_subappalto',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'id_subappalto': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_subappalto"))',
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['cig', 'id_aggiudicazione']
            },
            'stati_avanzamento': {
                'name': 'stati_avanzamento',
                'folder_glob': '*-stati-avanzamento_json',
                'json_pointer': 'item',
                'core_table': 'stati_avanzamento',
                'stg_json_table': 'stg_stati_avanzamento_json',
                'stg_table': 'stg_stati_avanzamento',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'varianti': {
                'name': 'varianti',
                'folder_glob': '*-varianti_json',
                'json_pointer': 'item',
                'core_table': 'varianti',
                'stg_json_table': 'stg_varianti_json',
                'stg_table': 'stg_varianti',
                'key': 'id_variante',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'id_variante': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_variante"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'fine_contratto': {
                'name': 'fine_contratto',
                'folder_glob': '*-fine-contratto_json',
                'json_pointer': 'item',
                'core_table': 'fine_contratto',
                'stg_json_table': 'stg_fine_contratto_json',
                'stg_table': 'stg_fine_contratto',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'collaudo': {
                'name': 'collaudo',
                'folder_glob': '*-collaudo_json',
                'json_pointer': 'item',
                'core_table': 'collaudo',
                'stg_json_table': 'stg_collaudo_json',
                'stg_table': 'stg_collaudo',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'quadro_economico': {
                'name': 'quadro_economico',
                'folder_glob': '*-quadro-economico_json',
                'json_pointer': 'item',
                'core_table': 'quadro_economico',
                'stg_json_table': 'stg_quadro_economico_json',
                'stg_table': 'stg_quadro_economico',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'fonti_finanziamento': {
                'name': 'fonti_finanziamento',
                'folder_glob': '*-fonti-finanziamento_json',
                'json_pointer': 'item',
                'core_table': 'fonti_finanziamento',
                'stg_json_table': 'stg_fonti_finanziamento_json',
                'stg_table': 'stg_fonti_finanziamento',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            },
            'avvio_contratto': {
                'name': 'avvio_contratto',
                'folder_glob': '*-avvio-contratto_json',
                'json_pointer': 'item',
                'core_table': 'avvio_contratto',
                'stg_json_table': 'stg_avvio_contratto_json',
                'stg_table': 'stg_avvio_contratto',
                'key': 'cig',
                'depends_on': ['aggiudicazioni'],
                'select_map': {
                    'cig': 'JSON_UNQUOTE(JSON_EXTRACT(payload, "$.cig"))',
                    'id_aggiudicazione': 'CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, "$.id_aggiudicazione")) AS INT)'
                },
                'upsert_update_fields': ['id_aggiudicazione']
            }
        }
    
    def discover_datasets(self) -> Dict[str, Any]:
        """Discover datasets in JSON folders and update registry"""
        logger.info("Starting dataset discovery")
        
        discovered_datasets = {}
        unknown_datasets = []
        
        if not self.config.json_root.exists():
            logger.warning(f"JSON root directory does not exist: {self.config.json_root}")
            return discovered_datasets
        
        # Scan for folders matching pattern YYYYMMDD-<dataset>_json
        pattern = re.compile(r'^\d{8}-(.+)_json$')
        
        for folder in self.config.json_root.iterdir():
            if folder.is_dir():
                match = pattern.match(folder.name)
                if match:
                    dataset_name = match.group(1)
                    
                    if dataset_name in self.predefined_datasets:
                        discovered_datasets[dataset_name] = self.predefined_datasets[dataset_name].copy()
                        discovered_datasets[dataset_name]['last_seen'] = folder.name
                        logger.info(f"Discovered known dataset: {dataset_name}")
                    else:
                        unknown_datasets.append({
                            'name': dataset_name,
                            'folder': folder.name,
                            'path': str(folder)
                        })
                        logger.warning(f"Discovered unknown dataset: {dataset_name}")
        
        # Update registry
        registry = {
            'datasets': discovered_datasets,
            'unknown_datasets': unknown_datasets,
            'discovery_timestamp': str(Path().cwd()),
            'json_root': str(self.config.json_root),
            'ndjson_root': str(self.config.ndjson_root)
        }
        
        self.config.update_registry(registry)
        
        # Print results
        print(f"Dataset Discovery Results:")
        print(f"Known datasets found: {len(discovered_datasets)}")
        for name, config in discovered_datasets.items():
            print(f"  - {name} (last seen: {config.get('last_seen', 'unknown')})")
        
        if unknown_datasets:
            print(f"\nUnknown datasets found: {len(unknown_datasets)}")
            for dataset in unknown_datasets:
                print(f"  - {dataset['name']} (folder: {dataset['folder']})")
        
        logger.info(f"Discovery completed. Found {len(discovered_datasets)} known and {len(unknown_datasets)} unknown datasets")
        
        return registry
    
    def export_registry(self):
        """Export registry to YAML config file"""
        logger.info("Exporting registry to YAML")
        
        registry = self.config.get_registry()
        
        if not registry:
            logger.warning("No registry data found. Run 'discover' command first.")
            return
        
        # Create comprehensive config structure
        config_data = {
            'database': {
                'host': self.config.db_host,
                'port': self.config.db_port,
                'name': self.config.db_name,
                'user': self.config.db_user
            },
            'paths': {
                'json_root': str(self.config.json_root),
                'ndjson_root': str(self.config.ndjson_root),
                'logs_root': str(self.config.logs_root)
            },
            'registry': registry,
            'etl': {
                'parallel_workers': 4,
                'batch_size': 1000,
                'max_retries': 3
            }
        }
        
        # Save to config file
        self.config.save_config()
        
        print(f"Registry exported to: {self.config.config_path}")
        print(f"Configured datasets: {len(registry.get('datasets', {}))}")
        
        logger.info("Registry export completed")
    
    def get_dataset_config(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific dataset"""
        registry = self.config.get_registry()
        datasets = registry.get('datasets', {})
        return datasets.get(dataset_name)
    
    def list_datasets(self) -> List[str]:
        """List all configured datasets"""
        registry = self.config.get_registry()
        datasets = registry.get('datasets', {})
        return list(datasets.keys())
    
    def validate_dataset_config(self, dataset_name: str) -> List[str]:
        """Validate dataset configuration and return any issues"""
        issues = []
        config = self.get_dataset_config(dataset_name)
        
        if not config:
            issues.append(f"Dataset '{dataset_name}' not found in registry")
            return issues
        
        required_fields = ['name', 'core_table', 'stg_table', 'key', 'select_map']
        for field in required_fields:
            if field not in config:
                issues.append(f"Missing required field: {field}")
        
        if 'depends_on' in config:
            for dep in config['depends_on']:
                if dep not in self.list_datasets():
                    issues.append(f"Dependency '{dep}' not found in registry")
        
        return issues
