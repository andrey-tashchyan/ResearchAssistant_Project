@echo off
REM ==============================================================================
REM PSID Pipeline - Environment Setup Script (Windows Batch)
REM ==============================================================================
REM Purpose: Simple batch file that calls the PowerShell setup script
REM Usage:   setup.bat
REM ==============================================================================

echo ========================================
echo PSID Pipeline - Windows Setup
echo ========================================
echo.
echo This will run the PowerShell setup script.
echo.

REM Check if PowerShell is available
where powershell >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: PowerShell is not available.
    echo Please run setup.ps1 directly in PowerShell.
    pause
    exit /b 1
)

REM Run PowerShell script with execution policy bypass
powershell -ExecutionPolicy Bypass -File "%~dp0setup.ps1"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ========================================
    echo Setup completed successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo Setup failed. Check errors above.
    echo ========================================
)

echo.
pause
