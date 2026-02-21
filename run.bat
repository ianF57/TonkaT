@echo off
title Assemblief Trader - Live Edition
echo.
echo  ==========================================
echo     Assemblief Trader  --  Live Edition
echo  ==========================================
echo.

cd /d "%~dp0assemblief-main"

where python >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Install it from https://python.org
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('python --version') do echo  Python: %%i

if exist ".venv" (
    echo  Removing old virtual environment...
    rmdir /s /q .venv
)

echo  Creating virtual environment...
python -m venv .venv

call .venv\Scripts\activate.bat

echo  Installing dependencies...
pip install -q -r requirements.txt

echo.
echo  Starting server at http://127.0.0.1:8000
echo  Browser will open automatically.
echo  Press Ctrl+C to stop.
echo.

python main.py
pause
