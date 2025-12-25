@echo off
setlocal enabledelayedexpansion

:: Get day of week (0=Sunday, 6=Saturday)
for /f %%a in ('powershell -command "(Get-Date).DayOfWeek.value__"') do set day=%%a

:: Get current hour (24-hour format)
for /f %%b in ('powershell -command "(Get-Date).Hour"') do set hour=%%b

:: Weekday check (1=Monday to 5=Friday)
if %day% lss 1 (
    echo Skipping: It's Sunday
    exit /b
)
if %day% gtr 5 (
    echo Skipping: It's Saturday
    exit /b
)

:: Time check: run only between 8am and 5pm (08–16 inclusive)
if %hour% lss 10 (
    echo Skipping: Before 10AM
    exit /b
)
if %hour% gtr 16 (
    echo Skipping: After 4PM
    exit /b
)

:: If passed checks, run your real command here

echo Running task...
cd C:\Users\joyag\Projects\ngx_tracker
:: python your_script.py

echo running scraper
python scraper.py

echo running analyser
python analyser.py

echo 🕵️ Running Institutional Watch...
python institutional_watch.py

echo 🔍 Running Weekly Trade Intelligence..30
python weekly_intel.py

echo Running Weekly 10
python weekly_intel_short.py

echo Running Weekly comparator
python intel_comparator.py

echo updating sector
python csv_db.py

echo 🔍 Running sector watch
python sector_institutional_watch.py

echo 🔍 Running sector tracker
python sector_tracker.py

echo 🔍 Running volume ranking
python volume_ranking.py



echo ########################################################

