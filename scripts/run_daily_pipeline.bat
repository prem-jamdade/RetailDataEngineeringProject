@echo off
setlocal
set PROJECT_DIR=%~dp0..
cd /d "%PROJECT_DIR%"
"%PROJECT_DIR%\.venv\Scripts\python.exe" -m src.pipeline --start-api
exit /b %ERRORLEVEL%
