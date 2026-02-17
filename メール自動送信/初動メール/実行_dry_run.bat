@echo off
chcp 65001 >nul
cd /d "%~dp0"
call venv\Scripts\activate
python -u auto_reply.py --dry-run
pause
