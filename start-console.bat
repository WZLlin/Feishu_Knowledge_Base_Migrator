@echo off
REM One-click start: double-click to launch the Web console (delegates to console.py).
python "%~dp0console.py" start
echo.
echo (You can close this window; the server keeps running in the background.)
pause
