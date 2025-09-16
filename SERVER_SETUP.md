# Setup Server Linux - ANAC Orchestrator

## Risoluzione Problemi Comuni

### 1. Problema Git Ownership e Permessi

Se ricevi errori come:
```
fatal: detected dubious ownership in repository at '/home/adm01/GIT/anac-orchestrator'
error: cannot open '.git/FETCH_HEAD': Permission denied
```

**Soluzione automatica (già integrata nello script):**
Lo script `avvio.sh` ora risolve automaticamente questi problemi.

**Soluzione manuale:**
```bash
# 1. Configura safe directory
git config --global --add safe.directory /home/adm01/GIT/anac-orchestrator

# 2. Correggi permessi
sudo chown -R $(whoami):$(whoami) .git
chmod -R 755 .git

# 3. Se ancora non funziona, riclona
cd ..
rm -rf anac-orchestrator
git clone https://github.com/pherdinauer/anac-orchestrator.git
cd anac-orchestrator
```

### 2. Problema File .env

Se ricevi errori come:
```
export: `user:': not a valid identifier
export: `psw:': not a valid identifier
```

**Causa:** Il file `.env` ha un formato sbagliato.

**Soluzione:**
1. Copia il file di esempio:
```bash
cp env_example.txt .env
```

2. Modifica il file `.env` con il formato corretto:
```bash
nano .env
```

**Formato corretto per .env:**
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_NAME=anac_db
DB_USER=anac_user
DB_PASSWORD=your_password_here

# Altri parametri...
```

**Formato SBAGLIATO (non usare):**
```bash
user: anac_user
psw: your_password
ip: localhost
port: 3306
```

### 3. Setup Completo Server

```bash
# 1. Clona il repository
git clone https://github.com/pherdinauer/anac-orchestrator.git
cd anac-orchestrator

# 2. Configura Git ownership
git config --global --add safe.directory $(pwd)

# 3. Crea il file .env
cp env_example.txt .env
nano .env  # Modifica con le tue credenziali

# 4. Rendi eseguibile lo script
chmod +x avvio.sh

# 5. Avvia il sistema
./avvio.sh
```

### 4. Configurazione Database

Nel file `.env`, configura:
```bash
DB_HOST=localhost          # IP del server MySQL
DB_PORT=3306              # Porta MySQL
DB_NAME=anac_db           # Nome database
DB_USER=anac_user         # Username MySQL
DB_PASSWORD=your_password # Password MySQL
```

### 5. Verifica Funzionamento

```bash
# Test del sistema
python3 test_system.py

# Verifica connessione database
python3 -c "from anac_orchestrator.config import Config; print('Config OK')"
```

### 6. Log e Debugging

I log sono salvati in:
- `anac_etl.log` - Log principale
- `database/logs/` - Log specifici per dataset

Per debug:
```bash
# Visualizza log in tempo reale
tail -f anac_etl.log

# Controlla errori
grep ERROR anac_etl.log
```

### 7. Comandi Utili

```bash
# Aggiorna il sistema
git pull origin main

# Reinstalla dipendenze
pip3 install -r requirements.txt --force-reinstall

# Reset completo
git reset --hard origin/main
pip3 install -r requirements.txt
```

## Troubleshooting

### Errore: "Permission denied"
```bash
chmod +x avvio.sh
chmod +x avvio.bat  # Se su Windows
```

### Errore: "Python not found"
```bash
# Installa Python 3
sudo apt update
sudo apt install python3 python3-pip
```

### Errore: "MySQL client not found"
```bash
# Installa MySQL client
sudo apt install mysql-client
```

### Errore: "Module not found"
```bash
# Reinstalla dipendenze
pip3 install -r requirements.txt --force-reinstall
```

## Supporto

Se hai problemi:
1. Controlla i log in `anac_etl.log`
2. Verifica la configurazione in `.env`
3. Assicurati che il database MySQL sia accessibile
4. Controlla i permessi dei file
