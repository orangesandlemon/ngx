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
if %hour% lss 8 (
    echo Skipping: Before 8AM
    exit /b
)
if %hour% gtr 16 (
    echo Skipping: After 5PM
    exit /b
)
@echo off
cd C:\Users\joyag\Projects\ngx_tracker\sweden
echo 🔍 Running Daily Swedish scrapper
python scraper_yahoo.py 
echo 🔍 Running swedish analyser
python analyser_se.py 
echo 🔍 Running institutional_watch_se
python institutional_watch_se.py

echo 🔍 Running Weekly Trade Intelligence..30
python weekly_intel.py

echo Running Weekly 10
python weekly_intel_short.py

echo Running Weekly comparator
python intel_comparator.py

echo All tasks completed.

