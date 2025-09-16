@echo off
setlocal enabledelayedexpansion

REM ANAC Orchestrator - Script di Avvio Automatico (Windows)
REM Questo script fa il pull dell'ultima versione e avvia il sistema

title ANAC Orchestrator - Auto Startup

echo.
echo ==========================================
echo     ANAC Orchestrator - Auto Startup
echo ==========================================
echo.

REM Verifica se siamo in un repository git
if not exist ".git" (
    echo [ERROR] Directory corrente non e' un repository git!
    echo [ERROR] Assicurati di essere nella directory del progetto ANAC Orchestrator
    pause
    exit /b 1
)

REM Carica variabili d'ambiente se il file .env esiste
if exist ".env" (
    echo [INFO] Caricamento configurazione da .env...
    for /f "usebackq tokens=1,2 delims==" %%a in (".env") do (
        if not "%%a"=="" if not "%%a:~0,1%"=="#" (
            set "%%a=%%b"
        )
    )
    echo [SUCCESS] Configurazione caricata
) else (
    echo [WARNING] File .env non trovato, usando configurazione di default
)

REM Verifica dipendenze
echo [INFO] Verifica dipendenze...

REM Verifica Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python non trovato! Installare Python 3.8+
    pause
    exit /b 1
)

REM Verifica pip
pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pip non trovato! Installare pip
    pause
    exit /b 1
)

REM Verifica se il virtual environment esiste
if not exist "venv" (
    echo [WARNING] Virtual environment non trovato!
    echo [INFO] Esecuzione setup virtual environment...
    if exist "setup_venv.bat" (
        call setup_venv.bat
    ) else (
        echo [ERROR] Script setup_venv.bat non trovato!
        echo [ERROR] Esegui manualmente: python -m venv venv
        pause
        exit /b 1
    )
)

REM Attiva virtual environment
echo [INFO] Attivazione virtual environment...
call venv\Scripts\activate.bat

echo [SUCCESS] Dipendenze verificate e virtual environment attivato

REM Pull dell'ultima versione
echo [INFO] Aggiornamento codice da GitHub...
git pull origin main
if errorlevel 1 (
    echo [ERROR] Errore durante il pull del codice
    pause
    exit /b 1
)
echo [SUCCESS] Codice aggiornato con successo

REM Installa/aggiorna dipendenze Python (nel virtual environment)
echo [INFO] Installazione dipendenze Python...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ERROR] Errore durante l'installazione delle dipendenze
    pause
    exit /b 1
)
echo [SUCCESS] Dipendenze Python installate

REM Installa il pacchetto in modalità development
echo [INFO] Installazione pacchetto ANAC Orchestrator...
pip install -e . --quiet
if errorlevel 1 (
    echo [ERROR] Errore durante l'installazione del pacchetto
    pause
    exit /b 1
)
echo [SUCCESS] Pacchetto installato

REM Crea directory necessarie
echo [INFO] Creazione directory necessarie...
if not exist "database\JSON" mkdir "database\JSON"
if not exist "database\NDJSON" mkdir "database\NDJSON"
if not exist "database\logs" mkdir "database\logs"
if not exist "config" mkdir "config"
echo [SUCCESS] Directory create

REM Menu interattivo
:menu
echo.
echo ==========================================
echo     ANAC Orchestrator - Menu Principale
echo ==========================================
echo.
echo Seleziona un'operazione:
echo 1^) Migrazione database (migrate up^)
echo 2^) Discovery dataset (discover^)
echo 3^) ETL Pipeline completa (run --all^)
echo 4^) ETL Pipeline con filtro data (run --since YYYYMM^)
echo 5^) Status sistema
echo 6^) Dry-run ETL
echo 7^) Test sistema
echo 8^) Esci
echo.
set /p choice="Inserisci la tua scelta (1-8): "

if "%choice%"=="1" (
    echo [INFO] Esecuzione migrazione database...
    anac-etl migrate up
    goto menu
)

if "%choice%"=="2" (
    echo [INFO] Esecuzione discovery dataset...
    anac-etl discover
    anac-etl registry export
    goto menu
)

if "%choice%"=="3" (
    echo [INFO] Esecuzione ETL pipeline completa...
    anac-etl ingest run --all
    goto menu
)

if "%choice%"=="4" (
    set /p date_filter="Inserisci la data (formato YYYYMM, es. 202402): "
    echo [INFO] Esecuzione ETL pipeline dal !date_filter!...
    anac-etl ingest run --since "!date_filter!"
    goto menu
)

if "%choice%"=="5" (
    echo [INFO] Stato sistema...
    anac-etl migrate status
    anac-etl ingest status
    goto menu
)

if "%choice%"=="6" (
    echo [INFO] Dry-run ETL...
    anac-etl ingest dry-run
    goto menu
)

if "%choice%"=="7" (
    echo [INFO] Test sistema...
    python test_system.py
    goto menu
)

if "%choice%"=="8" (
    echo [SUCCESS] Arrivederci!
    exit /b 0
)

echo [ERROR] Scelta non valida. Riprova.
goto menu
