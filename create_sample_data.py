#!/usr/bin/env python3
"""
Create sample JSON data for testing ANAC Orchestrator
"""

import json
from pathlib import Path
from datetime import datetime, timedelta

def create_sample_bando_cig():
    """Create sample bando_cig data"""
    data = [
        {
            "cig": "CIG001",
            "cf_amministrazione_appaltante": "CF001",
            "denominazione_amministrazione_appaltante": "Comune di Roma",
            "oggetto_gara": "Fornitura servizi informatici",
            "importo_complessivo_gara": 150000.00,
            "data_pubblicazione": "2024-02-01",
            "data_scadenza_offerta": "2024-03-01"
        },
        {
            "cig": "CIG002",
            "cf_amministrazione_appaltante": "CF002",
            "denominazione_amministrazione_appaltante": "Regione Lazio",
            "oggetto_gara": "Lavori di manutenzione strade",
            "importo_complessivo_gara": 500000.00,
            "data_pubblicazione": "2024-02-02",
            "data_scadenza_offerta": "2024-03-02"
        },
        {
            "cig": "CIG003",
            "cf_amministrazione_appaltante": "CF003",
            "denominazione_amministrazione_appaltante": "ASL Roma 1",
            "oggetto_gara": "Fornitura materiale sanitario",
            "importo_complessivo_gara": 75000.00,
            "data_pubblicazione": "2024-02-03",
            "data_scadenza_offerta": "2024-03-03"
        }
    ]
    return data

def create_sample_aggiudicazioni():
    """Create sample aggiudicazioni data"""
    data = [
        {
            "id_aggiudicazione": "AGG001",
            "cig": "CIG001",
            "importo_aggiudicazione": 145000.00,
            "data_aggiudicazione": "2024-03-15",
            "numero_offerte": 3
        },
        {
            "id_aggiudicazione": "AGG002",
            "cig": "CIG002",
            "importo_aggiudicazione": 480000.00,
            "data_aggiudicazione": "2024-03-20",
            "numero_offerte": 5
        },
        {
            "id_aggiudicazione": "AGG003",
            "cig": "CIG003",
            "importo_aggiudicazione": 72000.00,
            "data_aggiudicazione": "2024-03-25",
            "numero_offerte": 2
        }
    ]
    return data

def create_sample_aggiudicatari():
    """Create sample aggiudicatari data"""
    data = [
        {
            "id_aggiudicazioni": "AGG001",
            "cf_aggiudicatario": "CFCOMP001",
            "denominazione_aggiudicatario": "Tech Solutions S.r.l.",
            "importo_aggiudicazione": 145000.00
        },
        {
            "id_aggiudicazioni": "AGG002",
            "cf_aggiudicatario": "CFCOMP002",
            "denominazione_aggiudicatario": "Costruzioni Roma S.p.A.",
            "importo_aggiudicazione": 480000.00
        },
        {
            "id_aggiudicazioni": "AGG003",
            "cf_aggiudicatario": "CFCOMP003",
            "denominazione_aggiudicatario": "MedSupply S.r.l.",
            "importo_aggiudicazione": 72000.00
        }
    ]
    return data

def create_sample_cup():
    """Create sample CUP data"""
    data = [
        {
            "cig": "CIG001",
            "codice_cup": "CUP001",
            "descrizione_cup": "Servizi informatici e telematici"
        },
        {
            "cig": "CIG002",
            "codice_cup": "CUP002",
            "descrizione_cup": "Lavori di costruzione e manutenzione"
        },
        {
            "cig": "CIG003",
            "codice_cup": "CUP003",
            "descrizione_cup": "Forniture sanitarie e mediche"
        }
    ]
    return data

def main():
    """Create sample data structure"""
    print("Creating sample JSON data for ANAC Orchestrator...")
    
    # Create directory structure
    base_path = Path("database/JSON")
    base_path.mkdir(parents=True, exist_ok=True)
    
    # Create sample folders with today's date
    today = datetime.now().strftime("%Y%m%d")
    
    datasets = {
        "cig": create_sample_bando_cig(),
        "aggiudicazioni": create_sample_aggiudicazioni(),
        "aggiudicatari": create_sample_aggiudicatari(),
        "cup": create_sample_cup()
    }
    
    for dataset_name, data in datasets.items():
        # Create folder
        folder_name = f"{today}-{dataset_name}_json"
        folder_path = base_path / folder_name
        folder_path.mkdir(exist_ok=True)
        
        # Create JSON file
        json_file = folder_path / f"{dataset_name}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Created: {json_file} ({len(data)} records)")
    
    print(f"\nSample data created successfully!")
    print(f"Total datasets: {len(datasets)}")
    print(f"Base path: {base_path.absolute()}")
    
    print(f"\nYou can now test the system:")
    print(f"1. anac-etl discover")
    print(f"2. anac-etl ingest dry-run")
    print(f"3. anac-etl ingest convert --all")

if __name__ == "__main__":
    main()
