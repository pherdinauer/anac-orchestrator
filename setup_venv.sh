#!/bin/bash

# ANAC Orchestrator - Virtual Environment Setup Script
# This script creates and configures a virtual environment for the project

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
echo "  ANAC Orchestrator - Virtual Environment Setup"
echo "=========================================="
echo -e "${NC}"

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

# Verifica se venv è disponibile
if ! python3 -m venv --help &> /dev/null; then
    error "python3-venv non disponibile! Installare python3-venv"
    error "Su Ubuntu/Debian: sudo apt install python3-venv"
    error "Su CentOS/RHEL: sudo yum install python3-venv"
    exit 1
fi

# Crea virtual environment se non esiste
if [ ! -d "venv" ]; then
    log "Creazione virtual environment..."
    python3 -m venv venv
    success "Virtual environment creato"
else
    log "Virtual environment già esistente"
fi

# Attiva virtual environment
log "Attivazione virtual environment..."
source venv/bin/activate

# Aggiorna pip
log "Aggiornamento pip..."
pip install --upgrade pip --quiet

# Installa dipendenze
log "Installazione dipendenze Python..."
pip install -r requirements.txt --quiet

# Installa il pacchetto in modalità development
log "Installazione pacchetto ANAC Orchestrator..."
pip install -e . --quiet

success "Setup virtual environment completato!"
echo ""
echo -e "${GREEN}Per attivare il virtual environment in futuro, esegui:${NC}"
echo -e "${BLUE}  source venv/bin/activate${NC}"
echo ""
echo -e "${GREEN}Per disattivare il virtual environment:${NC}"
echo -e "${BLUE}  deactivate${NC}"
echo ""
