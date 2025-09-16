# ANAC Orchestrator

Sistema di gestione database e ETL per dati ANAC (Autorità Nazionale Anticorruzione).

## Caratteristiche

- **Gestione migrazioni schema**: Creazione e aggiornamento automatico del database
- **Discovery dataset**: Scansione automatica delle cartelle JSON per identificare i dataset
- **Pipeline ETL**: Conversione JSON → NDJSON → Staging → Core con gestione dipendenze
- **Gestione FK e indici**: Aggiunta automatica di foreign key e indici per ottimizzazione
- **Tabelle ETL**: Tracciamento completo delle operazioni ETL
- **CLI completa**: Comandi per tutte le operazioni

## Installazione

```bash
# Clona il repository
git clone <repository-url>
cd anac-orchestrator

# Installa le dipendenze
pip install -r requirements.txt

# Installa il pacchetto
pip install -e .
```

## Configurazione

### Variabili d'ambiente

```bash
export ANAC_DB_HOST=localhost
export ANAC_DB_PORT=3306
export ANAC_DB_NAME=anac_db
export ANAC_DB_USER=root
export ANAC_DB_PASSWORD=your_password
export ANAC_JSON_ROOT=database/JSON
export ANAC_NDJSON_ROOT=database/NDJSON
export ANAC_LOGS_ROOT=database/logs
export ANAC_SCRIPT_PATH=Script_creazioneDB_Anac.txt
```

### File di configurazione

Il sistema genera automaticamente `config/anac_etl.yml` con la configurazione dei dataset.

## Utilizzo

### 1. Migrazione Schema

```bash
# Applica tutte le migrazioni (v1: schema base, v2: FK + ETL + indici)
anac-etl migrate up

# Mostra stato migrazioni
anac-etl migrate status
```

### 2. Discovery Dataset

```bash
# Scansiona cartelle JSON e identifica dataset
anac-etl discover

# Esporta registry in YAML
anac-etl registry export
```

### 3. Pipeline ETL

```bash
# Conversione JSON → NDJSON
anac-etl ingest convert --all

# Caricamento NDJSON → Staging
anac-etl ingest load --all

# Upsert Staging → Core
anac-etl ingest upsert --all

# Pipeline completa
anac-etl ingest run --all

# Mostra stato ETL
anac-etl ingest status

# Simulazione (dry-run)
anac-etl ingest dry-run
```

### Filtri per dataset e date

```bash
# Processa solo un dataset specifico
anac-etl ingest run --dataset bando_cig

# Processa dataset da una data specifica
anac-etl ingest run --since 202402

# Processa tutti i dataset
anac-etl ingest run --all
```

## Struttura Database

### Tabelle Core

- `bando_cig`: Tabella principale con PK su `cig`
- `aggiudicazioni`: Aggiudicazioni con FK verso `bando_cig`
- `aggiudicatari`, `partecipanti`, `subappalti`, etc.: Tabelle figlie

### Tabelle ETL

- `etl_runs`: Tracciamento esecuzioni ETL
- `etl_files`: Tracciamento file processati
- `etl_rejects`: Record rifiutati con motivo

### Tabelle Staging

- `stg_<dataset>_json`: JSON raw per ogni dataset
- `stg_<dataset>`: Dati tipizzati per ogni dataset

## Struttura Directory

```
project/
├── anac_orchestrator/          # Codice sorgente
├── config/
│   └── anac_etl.yml           # Configurazione dataset
├── database/
│   ├── JSON/                  # File JSON originali
│   │   └── YYYYMMDD-<dataset>_json/
│   ├── NDJSON/                # File NDJSON convertiti
│   └── logs/                  # Log ETL
├── Script_creazioneDB_Anac.txt # Script schema originale
├── requirements.txt
└── setup.py
```

## Dataset Supportati

Il sistema supporta automaticamente tutti i dataset ANAC:

### Dataset Principali
- `cig`: Bandi e CIG (tabella principale)
- `aggiudicazioni`: Aggiudicazioni
- `aggiudicatari`: Aggiudicatari
- `partecipanti`: Partecipanti
- `cup`: Codici CUP
- `stazioni_appaltanti`: Stazioni appaltanti

### Dataset di Supporto
- `categorie_opera`: Categorie opera
- `categorie_dpcm_aggregazione`: Categorie DPCM
- `lavorazioni`: Lavorazioni
- `subappalti`: Subappalti
- `stati_avanzamento`: Stati avanzamento
- `varianti`: Varianti
- `fine_contratto`: Fine contratto
- `collaudo`: Collaudo
- `quadro_economico`: Quadro economico
- `fonti_finanziamento`: Fonti finanziamento
- `avvio_contratto`: Avvio contratto

### Struttura Cartelle
I dataset devono essere organizzati in cartelle con il pattern:
```
database/JSON/YYYYMMDD-dataset_name_json/
```
Esempio: `20240201-cig_json/`, `20240201-aggiudicazioni_json/`

## Gestione Dipendenze

Il sistema gestisce automaticamente l'ordine di caricamento:

1. **Padri**: `bando_cig`, stazioni appaltanti, CUP, categorie
2. **Figli diretti**: `aggiudicazioni`
3. **Figli di aggiudicazioni**: aggiudicatari, partecipanti, subappalti, etc.

## Gestione Errori

- **Idempotenza**: Rilanciare lo stesso file non crea duplicati
- **Gestione pending**: Record senza padre rimangono in staging
- **Logging completo**: Tutti gli errori sono tracciati
- **Rollback**: Transazioni con rollback automatico su errore

## Sviluppo

```bash
# Installa in modalità sviluppo
pip install -e .

# Esegui test
python -m pytest tests/

# Linting
flake8 anac_orchestrator/
```

## Script di Avvio Automatico

Il sistema include script di avvio che gestiscono automaticamente:
- Pull dell'ultima versione da GitHub
- Installazione dipendenze
- Menu interattivo per operazioni

**Linux/macOS:**
```bash
chmod +x avvio.sh
./avvio.sh
```

**Windows:**
```cmd
avvio.bat
```

## Configurazione Database

Crea un file `.env` basato su `.env.example`:
```bash
cp .env.example .env
# Modifica .env con le tue credenziali database
```

## Licenza

MIT License
