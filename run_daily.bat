@echo off
cd /d "%~dp0"

rem 防止 pyarrow 的 partition_alloc 在多线程并发时与 Windows Edge WebView2 冲突
set ARROW_DEFAULT_MEMORY_POOL=system

echo [%date% %time%] Starting daily workflow...

for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set TODAY=%%d
echo Today: %TODAY%

echo Step 1/3: Fetching market data...
python -m crawler.workflow daily
if errorlevel 1 (
    echo ERROR: market data fetch failed.
    exit /b 1
)

echo Step 2/3: Generating report...
python gen_report.py --market a
if errorlevel 1 (
    echo ERROR: report generation failed.
    exit /b 1
)

echo Step 3/4: Pushing report...
python push_report.py --market a
if errorlevel 1 (
    echo ERROR: push failed.
    exit /b 1
)

echo Step 4/4: Publishing to GitHub Pages...
git add reports\a\daily_%TODAY%.html reports\a\daily_%TODAY%.md reports\a\manifest.json index_a.html index.html
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "feat: daily report %TODAY%"
    git push origin main
    if errorlevel 1 (
        echo WARNING: git push failed, report saved locally only.
    )
) else (
    echo INFO: No new files to commit.
)

echo [%date% %time%] All done.
