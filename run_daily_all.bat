@echo off
cd /d "%~dp0"

set ARROW_DEFAULT_MEMORY_POOL=system

echo [%date% %time%] Starting all-market daily workflow...

for /f %%d in ('powershell -NoProfile -Command Get-Date -Format yyyy-MM-dd') do set TODAY=%%d
echo Today: %TODAY%

rem --- A share ---
echo.
echo [A-share] Step 1/3: Fetching market data...
python -m crawler.workflow daily
if errorlevel 1 (
    echo ERROR: A-share data fetch failed.
    exit /b 1
)

echo [A-share] Step 2/3: Generating report...
python gen_report.py --market a
if errorlevel 1 (
    echo ERROR: A-share report generation failed.
    exit /b 1
)

echo [A-share] Step 3/3: Pushing to Feishu...
python push_report.py --market a
if errorlevel 1 (
    echo WARNING: A-share push failed, continuing.
)

rem --- HK share ---
echo.
echo [HK] Step 1/3: Fetching market data...
python -m crawler.hk_workflow daily
if errorlevel 1 (
    echo ERROR: HK data fetch failed.
    exit /b 1
)

echo [HK] Step 2/3: Generating report...
python gen_report.py --market hk
if errorlevel 1 (
    echo ERROR: HK report generation failed.
    exit /b 1
)

echo [HK] Step 3/3: Pushing to Feishu...
python push_report.py --market hk
if errorlevel 1 (
    echo WARNING: HK push failed, continuing.
)

rem --- US share ---
echo.
echo [US] Step 1/3: Fetching market data...
python -m crawler.us_workflow daily
if errorlevel 1 (
    echo ERROR: US data fetch failed.
    exit /b 1
)

echo [US] Step 2/3: Generating report...
python gen_report.py --market us
if errorlevel 1 (
    echo ERROR: US report generation failed.
    exit /b 1
)

echo [US] Step 3/3: Pushing to Feishu...
python push_report.py --market us
if errorlevel 1 (
    echo WARNING: US push failed, continuing.
)

rem --- Update root index ---
echo.
echo [Index] Refreshing root index...
python gen_index.py --market all
if errorlevel 1 (
    echo WARNING: gen_index failed, continuing.
)

rem --- Git commit and push ---
echo.
echo [Git] Publishing to GitHub Pages...
git add reports\a\daily_%TODAY%.html reports\a\daily_%TODAY%.md reports\a\manifest.json reports\hk\hk_%TODAY%.html reports\hk\manifest.json reports\us\us_%TODAY%.html reports\us\manifest.json index_a.html index_hk.html index_us.html index.html

git diff --cached --quiet
if errorlevel 1 (
    git commit -m "feat: daily report %TODAY% (A+HK+US)"
    git push origin main
    if errorlevel 1 (
        echo WARNING: git push failed.
    )
) else (
    echo INFO: No new files to commit.
)

echo.
echo [%date% %time%] All done.
