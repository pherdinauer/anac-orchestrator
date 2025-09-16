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
    # Carica le variabili d'ambiente in modo sicuro
    set -a  # automatically export all variables
    source .env
    set +a  # stop automatically exporting
    success "Configurazione caricata"
else
    warning "File .env non trovato, usando configurazione di default"
    warning "Copia .env.example in .env e configura le tue credenziali"
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

# Verifica se il virtual environment esiste
if [ ! -d "venv" ]; then
    warning "Virtual environment non trovato!"
    log "Esecuzione setup virtual environment..."
    if [ -f "setup_venv.sh" ]; then
        chmod +x setup_venv.sh
        ./setup_venv.sh
    else
        error "Script setup_venv.sh non trovato!"
        error "Esegui manualmente: python3 -m venv venv"
        exit 1
    fi
fi

# Attiva virtual environment
log "Attivazione virtual environment..."
source venv/bin/activate

success "Dipendenze verificate e virtual environment attivato"

# Pull dell'ultima versione
log "Aggiornamento codice da GitHub..."

# Risolvi eventuali problemi di ownership Git
if ! git pull origin main 2>/dev/null; then
    warning "Problema con Git ownership rilevato"
    log "Tentativo di risoluzione automatica..."
    
    # Aggiungi la directory corrente come safe directory
    git config --global --add safe.directory "$(pwd)" 2>/dev/null || true
    
    # Controlla se ci sono problemi di permessi sui file .git
    if [ ! -w ".git/FETCH_HEAD" ] 2>/dev/null; then
        warning "Problemi di permessi sui file .git rilevati"
        log "Tentativo di correzione permessi..."
        
        # Prova a correggere i permessi (richiede sudo)
        if sudo chown -R $(whoami):$(whoami) .git 2>/dev/null; then
            success "Permessi .git corretti"
        else
            error "Impossibile correggere i permessi automaticamente"
            error "Esegui manualmente: sudo chown -R $(whoami):$(whoami) .git"
            error "Oppure riclona il repository: rm -rf . && git clone https://github.com/pherdinauer/anac-orchestrator.git ."
            exit 1
        fi
    fi
    
    # Riprova il pull
    if git pull origin main; then
        success "Codice aggiornato dopo risoluzione problemi Git"
    else
        error "Errore durante il pull del codice"
        error "Prova a riclonare il repository:"
        error "  cd .. && rm -rf anac-orchestrator"
        error "  git clone https://github.com/pherdinauer/anac-orchestrator.git"
        exit 1
    fi
else
    success "Codice aggiornato con successo"
fi

# Installa/aggiorna dipendenze Python (nel virtual environment)
log "Installazione dipendenze Python..."
if pip install -r requirements.txt --quiet; then
    success "Dipendenze Python installate"
else
    error "Errore durante l'installazione delle dipendenze"
    exit 1
fi

# Installa il pacchetto in modalità development
log "Installazione pacchetto ANAC Orchestrator..."
if pip install -e . --quiet; then
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
            python test_system.py
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
