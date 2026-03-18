@echo off
cd /d "%~dp0"

echo [%date% %time%] Starting daily workflow...

echo Step 1/3: Fetch market data
python -m crawler.workflow daily
if errorlevel 1 (
    echo ERROR: market data fetch failed.
    exit /b 1
)

echo Step 2/3: Generate report
python gen_report.py
if errorlevel 1 (
    echo ERROR: report generation failed.
    exit /b 1
)

echo Step 3/3: Push report
python push_report.py
if errorlevel 1 (
    echo ERROR: push failed.
    exit /b 1
)

echo [%date% %time%] Done.
