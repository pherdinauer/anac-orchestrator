@echo off
setlocal enabledelayedexpansion

REM ANAC Orchestrator - Virtual Environment Setup Script (Windows)
REM This script creates and configures a virtual environment for the project

title ANAC Orchestrator - Virtual Environment Setup

echo.
echo ==========================================
echo   ANAC Orchestrator - Virtual Environment Setup
echo ==========================================
echo.

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

REM Crea virtual environment se non esiste
if not exist "venv" (
    echo [INFO] Creazione virtual environment...
    python -m venv venv
    echo [SUCCESS] Virtual environment creato
) else (
    echo [INFO] Virtual environment già esistente
)

REM Attiva virtual environment
echo [INFO] Attivazione virtual environment...
call venv\Scripts\activate.bat

REM Aggiorna pip
echo [INFO] Aggiornamento pip...
python -m pip install --upgrade pip --quiet

REM Installa dipendenze
echo [INFO] Installazione dipendenze Python...
pip install -r requirements.txt --quiet

REM Installa il pacchetto in modalità development
echo [INFO] Installazione pacchetto ANAC Orchestrator...
pip install -e . --quiet

echo [SUCCESS] Setup virtual environment completato!
echo.
echo Per attivare il virtual environment in futuro, esegui:
echo   venv\Scripts\activate.bat
echo.
echo Per disattivare il virtual environment:
echo   deactivate
echo.
pause
