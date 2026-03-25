@echo off
cd /d "%~dp0"

echo Publishing all reports to GitHub Pages...

git add reports\*.html reports\*.md reports\manifest.json index.html .nojekyll
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "chore: publish reports"
    git push origin main
    if errorlevel 1 (
        echo ERROR: git push failed.
        exit /b 1
    )
    echo Done.
) else (
    echo INFO: Nothing to publish, all files already up to date.
)
