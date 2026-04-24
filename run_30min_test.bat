@echo off
chcp 65001 >nul
echo ========================================
echo  BOT 30-MINUTE RUNTIME TEST
echo ========================================
echo.

:: Check if already running
if exist bot.pid (
    echo WARNING: bot.pid exists. Checking if process is still running...
    for /f "tokens=*" %%a in (bot.pid) do (
        tasklist /FI "PID eq %%a" 2>nul | find "%%a" >nul
        if errorlevel 1 (
            echo Old process not running. Removing stale PID file.
            del bot.pid
        ) else (
            echo Bot is already running with PID %%a
            echo Please stop it first with: taskkill /PID %%a /F
            exit /b 1
        )
    )
)

:: Create log filename with timestamp
for /f "tokens=2 delims==." %%a in ('wmic os get localdatetime /value') do set datetime=%%a
set TIMESTAMP=%datetime:~0,8%_%datetime:~8,6%
set LOGFILE=bot_runtime_%TIMESTAMP%.log

echo Log file: %LOGFILE%
echo.

:: Start bot in background with output redirected to log
start /B python main.py > %LOGFILE% 2>&1

:: Give it a moment to start
ping -n 3 127.0.0.1 >nul

:: Get the PID
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq python.exe" /FO LIST ^| findstr "PID:"') do (
    echo %%a > bot.pid
    goto :found_pid
)

:found_pid
for /f %%a in (bot.pid) do echo Bot started with PID: %%a
echo.

:: Wait for bot to initialize
echo Waiting for bot to initialize (5 seconds)...
ping -n 6 127.0.0.1 >nul

:: Check if process is still running
for /f %%a in (bot.pid) do (
    tasklist /FI "PID eq %%a" 2>nul | find "%%a" >nul
    if errorlevel 1 (
        echo ERROR: Bot process not found! Check log file for errors.
        echo.
        echo Last 20 lines from log:
        type %LOGFILE% | findstr /N "." | findstr "^[0-9]*:1[0-9][0-9]" 2>nul || type %LOGFILE%
        del bot.pid 2>nul
        exit /b 1
    )
)

echo Bot is running. Starting 30-minute monitoring...
echo.

:: Run monitoring
python scripts\monitor_runtime.py

:: Cleanup
echo.
echo Test complete. Cleaning up...
if exist bot.pid del bot.pid

echo.
echo Log file saved: %LOGFILE%
echo.
pause
