# Risoluzione Automatica Conflitti Git

Lo script `avvio.sh` ora gestisce automaticamente i conflitti Git che si verificano durante il pull del codice.

## 🔧 **Come Funziona**

### **1. Rilevamento Modifiche Locali**
Lo script controlla automaticamente se ci sono modifiche locali non committate:
- File modificati (`git diff`)
- File in staging (`git diff --cached`)

### **2. Gestione File di Configurazione**
Per i file di configurazione specifici (`.env`, `.env.example`, `config/anac_etl.yml`):
- **Mantiene le versioni locali** per evitare la perdita di configurazioni personalizzate
- **Non li include nello stash** per preservare le impostazioni

### **3. Stash Automatico**
Per tutti gli altri file:
- **Salva automaticamente** le modifiche locali in uno stash
- **Esegue il pull** dal repository remoto
- **Ripristina le modifiche** locali se possibile

### **4. Gestione Conflitti**
Se ci sono conflitti durante il ripristino:
- **Salva le modifiche in stash** per riferimento futuro
- **Continua l'esecuzione** senza bloccare il sistema
- **Fornisce istruzioni** per recuperare le modifiche

## 📋 **Comandi Utili**

### **Visualizzare Stash Salvati**
```bash
git stash list
```

### **Recuperare Modifiche da Stash**
```bash
# Vedi le modifiche in uno stash
git stash show -p stash@{0}

# Applica le modifiche
git stash apply stash@{0}

# Rimuovi lo stash dopo l'applicazione
git stash drop stash@{0}
```

### **Gestione Manuale Conflitti**
Se la risoluzione automatica non funziona:
```bash
# Stash manuale
git stash push -m "Descrizione modifiche"

# Pull
git pull origin main

# Ripristino
git stash pop
```

## ⚠️ **File Gestiti Automaticamente**

### **File di Configurazione (Mantenuti Localmente)**
- `.env` - Variabili d'ambiente personalizzate
- `.env.example` - Template variabili d'ambiente
- `config/anac_etl.yml` - Configurazione ETL

### **File Ignorati da Git**
- `venv/` - Virtual environment Python
- `*.log` - File di log
- `database/JSON/*/data/` - Dati del database
- `database/NDJSON/*/data/` - Dati NDJSON

## 🎯 **Vantaggi**

1. **Nessun Intervento Manuale** - Lo script gestisce tutto automaticamente
2. **Preservazione Configurazioni** - Le impostazioni locali non vengono perse
3. **Aggiornamenti Sicuri** - Il codice viene sempre aggiornato dal repository
4. **Recupero Facile** - Le modifiche sono sempre salvate in stash
5. **Logging Dettagliato** - Ogni operazione è tracciata nei log

## 🔍 **Troubleshooting**

### **Errore: "Your local changes would be overwritten"**
Questo errore non dovrebbe più verificarsi. Se persiste:
1. Controlla i log dello script per dettagli
2. Usa `git stash list` per vedere le modifiche salvate
3. Applica manualmente le modifiche se necessario

### **Modifiche Perdute**
Le modifiche non vengono mai perse:
1. Controlla `git stash list`
2. Usa `git stash show -p stash@{N}` per vedere le modifiche
3. Applica con `git stash apply stash@{N}`

### **Conflitti Persistenti**
Se i conflitti persistono:
1. Esegui `git status` per vedere lo stato
2. Risolvi manualmente i conflitti
3. Committa le modifiche risolte
