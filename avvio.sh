#!/bin/bash

# ANAC Orchestrator - Script di Avvio Automatico
# Questo script fa il pull dell'ultima versione e avvia il sistema

set -e  # Exit on any error

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funzione per logging
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Banner
echo -e "${GREEN}"
echo "=========================================="
echo "    ANAC Orchestrator - Auto Startup"
echo "=========================================="
echo -e "${NC}"

# Verifica se siamo in un repository git
if [ ! -d ".git" ]; then
    error "Directory corrente non è un repository git!"
    error "Assicurati di essere nella directory del progetto ANAC Orchestrator"
    exit 1
fi

# Carica variabili d'ambiente se il file .env esiste
if [ -f ".env" ]; then
    log "Caricamento configurazione da .env..."
    export $(cat .env | grep -v '^#' | xargs)
    success "Configurazione caricata"
else
    warning "File .env non trovato, usando configurazione di default"
fi

# Verifica dipendenze
log "Verifica dipendenze..."

# Verifica Python
if ! command -v python3 &> /dev/null; then
    error "Python3 non trovato! Installare Python 3.8+"
    exit 1
fi

# Verifica pip
if ! command -v pip3 &> /dev/null; then
    error "pip3 non trovato! Installare pip"
    exit 1
fi

# Verifica MySQL client
if ! command -v mysql &> /dev/null; then
    warning "MySQL client non trovato, alcune funzionalità potrebbero non funzionare"
fi

success "Dipendenze verificate"

# Pull dell'ultima versione
log "Aggiornamento codice da GitHub..."
if git pull origin main; then
    success "Codice aggiornato con successo"
else
    error "Errore durante il pull del codice"
    exit 1
fi

# Installa/aggiorna dipendenze Python
log "Installazione dipendenze Python..."
if pip3 install -r requirements.txt --quiet; then
    success "Dipendenze Python installate"
else
    error "Errore durante l'installazione delle dipendenze"
    exit 1
fi

# Installa il pacchetto in modalità development
log "Installazione pacchetto ANAC Orchestrator..."
if pip3 install -e . --quiet; then
    success "Pacchetto installato"
else
    error "Errore durante l'installazione del pacchetto"
    exit 1
fi

# Verifica connessione database
log "Verifica connessione database..."
if [ -n "$ANAC_DB_HOST" ] && [ -n "$ANAC_DB_NAME" ]; then
    if mysql -h"$ANAC_DB_HOST" -P"$ANAC_DB_PORT" -u"$ANAC_DB_USER" -p"$ANAC_DB_PASSWORD" -e "SELECT 1;" "$ANAC_DB_NAME" &>/dev/null; then
        success "Connessione database OK"
    else
        warning "Impossibile connettersi al database. Verificare le credenziali in .env"
    fi
else
    warning "Configurazione database non completa, saltando verifica"
fi

# Crea directory necessarie
log "Creazione directory necessarie..."
mkdir -p database/JSON
mkdir -p database/NDJSON
mkdir -p database/logs
mkdir -p config
success "Directory create"

# Menu interattivo
echo -e "${GREEN}"
echo "=========================================="
echo "    ANAC Orchestrator - Menu Principale"
echo "=========================================="
echo -e "${NC}"

while true; do
    echo ""
    echo "Seleziona un'operazione:"
    echo "1) Migrazione database (migrate up)"
    echo "2) Discovery dataset (discover)"
    echo "3) ETL Pipeline completa (run --all)"
    echo "4) ETL Pipeline con filtro data (run --since YYYYMM)"
    echo "5) Status sistema"
    echo "6) Dry-run ETL"
    echo "7) Test sistema"
    echo "8) Esci"
    echo ""
    read -p "Inserisci la tua scelta (1-8): " choice

    case $choice in
        1)
            log "Esecuzione migrazione database..."
            anac-etl migrate up
            ;;
        2)
            log "Esecuzione discovery dataset..."
            anac-etl discover
            anac-etl registry export
            ;;
        3)
            log "Esecuzione ETL pipeline completa..."
            anac-etl ingest run --all
            ;;
        4)
            read -p "Inserisci la data (formato YYYYMM, es. 202402): " date_filter
            log "Esecuzione ETL pipeline dal $date_filter..."
            anac-etl ingest run --since "$date_filter"
            ;;
        5)
            log "Stato sistema..."
            anac-etl migrate status
            anac-etl ingest status
            ;;
        6)
            log "Dry-run ETL..."
            anac-etl ingest dry-run
            ;;
        7)
            log "Test sistema..."
            python3 test_system.py
            ;;
        8)
            success "Arrivederci!"
            exit 0
            ;;
        *)
            error "Scelta non valida. Riprova."
            ;;
    esac
done
