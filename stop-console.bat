@echo off
REM One-click stop: double-click to shut down the Web console (delegates to console.py).
python "%~dp0console.py" stop
echo.
pause
