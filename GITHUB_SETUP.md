# Setup GitHub Repository

## Istruzioni per creare e configurare il repository GitHub

### 1. Crea il repository su GitHub

1. Vai su [GitHub](https://github.com) e accedi al tuo account
2. Clicca su "New repository" o vai su https://github.com/new
3. Nome repository: `anac-orchestrator`
4. Descrizione: `ANAC Orchestrator - Sistema di gestione database e ETL per dati ANAC`
5. Imposta come **Public** o **Private** a tua scelta
6. **NON** inizializzare con README, .gitignore o licenza (già presenti)
7. Clicca "Create repository"

### 2. Inizializza il repository locale

```bash
# Inizializza git (se non già fatto)
git init

# Aggiungi il remote origin
git remote add origin https://github.com/TUO_USERNAME/anac-orchestrator.git

# Aggiungi tutti i file
git add .

# Prima commit
git commit -m "Initial commit: ANAC Orchestrator complete implementation"

# Push al repository
git push -u origin main
```

### 3. Configura il file .env

```bash
# Copia il file di esempio
cp .env.example .env

# Modifica .env con le tue credenziali database
nano .env  # o usa il tuo editor preferito
```

### 4. Aggiorna gli script di avvio

Modifica `avvio.sh` e `avvio.bat` per puntare al tuo repository:

```bash
# In avvio.sh, aggiorna questa riga:
git pull origin main
```

### 5. Test del sistema

```bash
# Testa che tutto funzioni
python test_system.py

# Testa lo script di avvio
./avvio.sh  # Linux/macOS
# oppure
avvio.bat   # Windows
```

### 6. Struttura del repository

Il repository contiene:

```
anac-orchestrator/
├── anac_orchestrator/          # Codice sorgente Python
├── config/                     # Configurazioni
├── database/                   # Directory per dati
├── Script_creazioneDB_Anac.txt # Script schema database
├── requirements.txt            # Dipendenze Python
├── setup.py                    # Setup package
├── avvio.sh                    # Script avvio Linux/macOS
├── avvio.bat                   # Script avvio Windows
├── .env.example                # Template configurazione
├── README.md                   # Documentazione
└── .gitignore                  # File da ignorare
```

### 7. Comandi utili

```bash
# Aggiorna il repository
git add .
git commit -m "Descrizione delle modifiche"
git push origin main

# Pull delle modifiche
git pull origin main

# Verifica stato
git status
git log --oneline
```

### 8. Configurazione automatica

Gli script di avvio (`avvio.sh` e `avvio.bat`) gestiscono automaticamente:
- Pull dell'ultima versione
- Installazione dipendenze
- Menu interattivo per operazioni

### 9. Note importanti

- **Non committare** il file `.env` (contiene credenziali sensibili)
- **Non committare** i dati JSON reali (solo la struttura delle cartelle)
- Il file `.gitignore` è già configurato correttamente
- Usa sempre `git pull` prima di iniziare a lavorare

### 10. Prossimi passi

1. Configura il database MySQL
2. Posiziona i file JSON reali in `database/JSON/`
3. Esegui `anac-etl migrate up` per creare lo schema
4. Esegui `anac-etl discover` per configurare i dataset
5. Esegui `anac-etl ingest run --all` per la pipeline completa

Il sistema è pronto per l'uso in produzione! 🚀
