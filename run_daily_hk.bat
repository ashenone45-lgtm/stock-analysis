@echo off
cd /d "%~dp0"

rem 防止 pyarrow 的 partition_alloc 在多线程并发时与 Windows Edge WebView2 冲突
set ARROW_DEFAULT_MEMORY_POOL=system

echo [%date% %time%] Starting HK daily workflow...

for /f %%d in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set TODAY=%%d
echo Today: %TODAY%

echo Step 1/4: Fetching HK market data...
python -m crawler.hk_workflow daily
if errorlevel 1 (
    echo ERROR: HK market data fetch failed.
    exit /b 1
)

echo Step 2/4: Generating HK report...
python gen_report.py --market hk
if errorlevel 1 (
    echo ERROR: HK report generation failed.
    exit /b 1
)

echo Step 3/4: Publishing to GitHub Pages...
git add reports\hk\hk_%TODAY%.html reports\hk\manifest.json index_hk.html index.html
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "feat: HK daily report %TODAY%"
    git push origin main
    if errorlevel 1 (
        echo WARNING: git push failed, report saved locally only.
    )
) else (
    echo INFO: No new files to commit.
)

echo [%date% %time%] All done.
