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
if %hour% lss 14 (
    echo Skipping: Before 2PM
    exit /b
)
if %hour% gtr 22 (
    echo Skipping: After 10PM
    exit /b
)

@echo off
cd C:\Users\joyag\Projects\ngx_tracker\us
python us_analyser.py 
