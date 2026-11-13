@echo off
chcp 65001 >nul
title Realtime Monitor
cd /d "%~dp0"

echo ==========================================
echo   Realtime Info / Price Monitor
echo ==========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] python not found in PATH
    echo Please install Python 3.9+ first.
    pause
    exit /b 1
)

echo Starting server...
start "" /min cmd /c "timeout /t 3 >nul && start http://127.0.0.1:8848/"

echo Dashboard: http://127.0.0.1:8848/
echo.
echo Press Ctrl+C or close this window to stop.
echo ==========================================
echo.

python run.py

echo.
echo Server stopped.
pause
