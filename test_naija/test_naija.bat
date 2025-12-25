
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

cd C:\Users\joyag\Projects\ngx_tracker\test_naija

echo 🕵️ Running scraper.
python scraper.py

echo 🔍 Running Weekly Trade Intelligence...
python weekly_intel.py

echo 🔍 Running institutional_watch
python institutional_watch.py

echo 🕵️ Running analyser
python analyser_test_naija.py

echo ✅ All tasks completed.


