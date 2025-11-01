# ==============================================================================
# PSID Pipeline - Environment Setup Script (Windows PowerShell)
# ==============================================================================
# Purpose: Automated virtual environment creation and dependency installation
#          for the PSID data processing pipeline (academic research tool)
#
# Usage:   .\setup.ps1
#          (If you get an error, run: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser)
#
# Features:
#   - Creates Python virtual environment (psid_env)
#   - Installs all required dependencies
#   - Color-coded progress output
#   - Progress indicators for long operations
#   - Windows compatible
#   - Safe error handling and validation
#
# Author:  PSID Pipeline Team
# Date:    2024-11-01
# ==============================================================================

# Stop on errors
$ErrorActionPreference = "Stop"

# ==============================================================================
# CONFIGURATION
# ==============================================================================

$VENV_NAME = "psid_env"
$REQUIREMENTS_FILE = "requirements.txt"
$PYTHON_MIN_VERSION = [version]"3.8"

# Default dependencies if requirements.txt doesn't exist
$DEFAULT_PACKAGES = @(
    "pandas>=1.5.0",
    "numpy>=1.23.0",
    "pyarrow>=10.0.0",
    "openpyxl>=3.0.0",
    "tqdm>=4.65.0",
    "regex>=2022.10.31",
    "multiprocess>=0.70.14"
)

# ==============================================================================
# COLOR FUNCTIONS
# ==============================================================================

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-ColorOutput "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" "Cyan"
    Write-ColorOutput "  $Message" "Cyan"
    Write-ColorOutput "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" "Cyan"
    Write-Host ""
}

function Write-Step {
    param([string]$Message)
    Write-ColorOutput "→ $Message" "Blue"
}

function Write-Success {
    param([string]$Message)
    Write-ColorOutput "✓ $Message" "Green"
}

function Write-Skip {
    param([string]$Message)
    Write-ColorOutput "⊘ $Message" "Yellow"
}

function Write-ErrorMsg {
    param([string]$Message)
    Write-ColorOutput "✗ ERROR: $Message" "Red"
}

function Write-Info {
    param([string]$Message)
    Write-ColorOutput "  ℹ $Message" "Gray"
}

function Write-Warning {
    param([string]$Message)
    Write-ColorOutput "⚠ $Message" "Yellow"
}

# ==============================================================================
# VALIDATION FUNCTIONS
# ==============================================================================

function Test-PythonInstallation {
    Write-Step "Checking Python installation..."

    # Try to find Python
    $pythonCmd = $null
    $pythonPaths = @("python", "python3", "py")

    foreach ($cmd in $pythonPaths) {
        try {
            $version = & $cmd --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                $pythonCmd = $cmd
                break
            }
        }
        catch {
            continue
        }
    }

    if (-not $pythonCmd) {
        Write-ErrorMsg "Python is not installed or not in PATH"
        Write-Info "Please install Python $PYTHON_MIN_VERSION+ from https://www.python.org/"
        Write-Info "Make sure to check 'Add Python to PATH' during installation"
        exit 1
    }

    # Get Python version
    $versionOutput = & $pythonCmd --version 2>&1
    $versionMatch = $versionOutput -match "Python (\d+\.\d+\.\d+)"

    if ($versionMatch) {
        $pythonVersion = [version]$matches[1]

        if ($pythonVersion -lt $PYTHON_MIN_VERSION) {
            Write-ErrorMsg "Python $PYTHON_MIN_VERSION+ required (found $pythonVersion)"
            exit 1
        }

        Write-Success "Found Python $pythonVersion at $(Get-Command $pythonCmd | Select-Object -ExpandProperty Source)"
        return $pythonCmd
    }
    else {
        Write-ErrorMsg "Could not determine Python version"
        exit 1
    }
}

function Test-OperatingSystem {
    Write-Step "Checking operating system..."

    $os = [System.Environment]::OSVersion.Platform
    if ($os -eq "Win32NT") {
        $osVersion = [System.Environment]::OSVersion.Version
        Write-Success "Windows $($osVersion.Major).$($osVersion.Minor) detected"
    }
    else {
        Write-Warning "Unexpected OS: $os (script may still work)"
    }
}

# ==============================================================================
# VIRTUAL ENVIRONMENT FUNCTIONS
# ==============================================================================

function New-VirtualEnvironment {
    param([string]$PythonCmd)

    if (Test-Path $VENV_NAME) {
        Write-Skip "Virtual environment '$VENV_NAME' already exists"
        return $true
    }

    Write-Step "Creating virtual environment '$VENV_NAME'..."

    try {
        & $PythonCmd -m venv $VENV_NAME

        if ($LASTEXITCODE -eq 0) {
            Write-Success "Virtual environment created: $VENV_NAME\"
            return $true
        }
        else {
            Write-ErrorMsg "Failed to create virtual environment (exit code: $LASTEXITCODE)"
            return $false
        }
    }
    catch {
        Write-ErrorMsg "Failed to create virtual environment: $_"
        return $false
    }
}

function Enable-VirtualEnvironment {
    Write-Step "Activating virtual environment..."

    $activateScript = Join-Path $VENV_NAME "Scripts\Activate.ps1"

    if (-not (Test-Path $activateScript)) {
        Write-ErrorMsg "Activation script not found: $activateScript"
        exit 1
    }

    try {
        & $activateScript

        Write-Success "Virtual environment activated"
        Write-Info "Active Python: $(Get-Command python | Select-Object -ExpandProperty Source)"
        return $true
    }
    catch {
        Write-ErrorMsg "Failed to activate virtual environment: $_"
        return $false
    }
}

# ==============================================================================
# DEPENDENCY INSTALLATION
# ==============================================================================

function Update-Pip {
    Write-Step "Upgrading pip..."

    try {
        python -m pip install --upgrade pip --quiet

        if ($LASTEXITCODE -eq 0) {
            $pipVersion = (pip --version).Split()[1]
            Write-Success "pip upgraded to version $pipVersion"
            return $true
        }
        else {
            Write-ErrorMsg "Failed to upgrade pip (exit code: $LASTEXITCODE)"
            return $false
        }
    }
    catch {
        Write-ErrorMsg "Failed to upgrade pip: $_"
        return $false
    }
}

function New-DefaultRequirements {
    Write-Step "Creating default requirements.txt..."

    $content = @"
# PSID Pipeline - Python Dependencies
# Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

# Core data processing
"@

    foreach ($package in $DEFAULT_PACKAGES) {
        $content += "`n$package"
    }

    Set-Content -Path $REQUIREMENTS_FILE -Value $content -Encoding UTF8

    Write-Success "Created $REQUIREMENTS_FILE with default packages"
    Write-Info "File location: $(Get-Location)\$REQUIREMENTS_FILE"
}

function Install-Dependencies {
    if (-not (Test-Path $REQUIREMENTS_FILE)) {
        Write-Warning "requirements.txt not found"
        New-DefaultRequirements
    }
    else {
        Write-Step "Found existing $REQUIREMENTS_FILE"
    }

    # Count packages
    $packageCount = (Get-Content $REQUIREMENTS_FILE | Where-Object { $_ -match '^[^#]' -and $_.Trim() }).Count

    Write-Step "Installing dependencies from $REQUIREMENTS_FILE..."
    Write-Info "Installing $packageCount package(s)..."

    try {
        python -m pip install -r $REQUIREMENTS_FILE

        if ($LASTEXITCODE -eq 0) {
            Write-Success "All dependencies installed successfully"

            # Show installed packages
            Write-Info "Installed packages:"
            Get-Content $REQUIREMENTS_FILE | Where-Object { $_ -match '^[^#]' -and $_.Trim() } | ForEach-Object {
                $pkgName = ($_ -split '[><=\[]')[0].Trim()
                try {
                    $pkgVersion = (pip show $pkgName 2>$null | Select-String "Version:").ToString().Split()[1]
                    Write-Host "  • $pkgName " -NoNewline -ForegroundColor Gray
                    Write-Host $pkgVersion -ForegroundColor Green
                }
                catch {
                    # Skip if package info not available
                }
            }
            return $true
        }
        else {
            Write-ErrorMsg "Failed to install dependencies (exit code: $LASTEXITCODE)"
            Write-Info "Try running: pip install -r $REQUIREMENTS_FILE"
            return $false
        }
    }
    catch {
        Write-ErrorMsg "Failed to install dependencies: $_"
        return $false
    }
}

# ==============================================================================
# VERIFICATION
# ==============================================================================

function Test-Installation {
    Write-Step "Verifying installation..."

    $criticalPackages = @("pandas", "numpy", "pyarrow")
    $allOk = $true

    foreach ($package in $criticalPackages) {
        try {
            $result = python -c "import $package; print($package.__version__)" 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  ✓ $package " -NoNewline -ForegroundColor Green
                Write-Host "($result)" -ForegroundColor Gray
            }
            else {
                Write-Host "  ✗ $package " -NoNewline -ForegroundColor Red
                Write-Host "(import failed)" -ForegroundColor Gray
                $allOk = $false
            }
        }
        catch {
            Write-Host "  ✗ $package " -NoNewline -ForegroundColor Red
            Write-Host "(import failed)" -ForegroundColor Gray
            $allOk = $false
        }
    }

    if ($allOk) {
        Write-Success "All critical packages verified"
    }
    else {
        Write-Warning "Some packages failed verification (may still work)"
    }
}

# ==============================================================================
# FINAL MESSAGES
# ==============================================================================

function Write-FinalMessage {
    Write-Host ""
    Write-ColorOutput "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" "Green"
    Write-ColorOutput "  ✅ PSID environment ready" "Green"
    Write-ColorOutput "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" "Green"
    Write-Host ""
    Write-Host "To activate the environment, run:" -ForegroundColor White
    Write-ColorOutput "  .\$VENV_NAME\Scripts\Activate.ps1" "Cyan"
    Write-Host ""
    Write-Host "To deactivate when done:" -ForegroundColor White
    Write-ColorOutput "  deactivate" "Cyan"
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor White
    Write-Host "  1. Activate environment (see above)" -ForegroundColor Gray
    Write-Host "  2. Run individual Python scripts" -ForegroundColor Gray
    Write-Host "  3. Or use: " -NoNewline -ForegroundColor Gray
    Write-ColorOutput "bash run_all.sh" "Cyan" -NoNewline
    Write-Host " (requires Git Bash/WSL)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Project location: $(Get-Location)" -ForegroundColor Gray
    Write-Host ""
}

function Write-Summary {
    param([datetime]$StartTime)

    $elapsed = (Get-Date) - $StartTime

    Write-Host ""
    Write-Host "Setup Summary:" -ForegroundColor White
    Write-Host "  • Virtual environment: " -NoNewline -ForegroundColor Gray
    Write-ColorOutput "$VENV_NAME\" "Green"
    Write-Host "  • Python version: " -NoNewline -ForegroundColor Gray
    $pyVersion = python --version 2>&1
    Write-ColorOutput $pyVersion.Replace("Python ", "") "Green"
    Write-Host "  • Dependencies: " -NoNewline -ForegroundColor Gray
    $pkgCount = (pip list --format=freeze | Measure-Object).Count
    Write-ColorOutput "$pkgCount packages installed" "Green"
    Write-Host "  • Time elapsed: " -NoNewline -ForegroundColor Gray
    Write-ColorOutput "$([math]::Round($elapsed.TotalSeconds))s" "Green"
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

function Main {
    $startTime = Get-Date

    # Print banner
    Write-Header "🚀 PSID Pipeline - Environment Setup (Windows)"

    Write-Host "This script will:" -ForegroundColor White
    Write-Host "  1. Validate Python installation" -ForegroundColor Gray
    Write-Host "  2. Create virtual environment ($VENV_NAME)" -ForegroundColor Gray
    Write-Host "  3. Install/upgrade pip" -ForegroundColor Gray
    Write-Host "  4. Install project dependencies" -ForegroundColor Gray
    Write-Host "  5. Verify installation" -ForegroundColor Gray
    Write-Host ""

    # Pre-flight checks
    Test-OperatingSystem
    $pythonCmd = Test-PythonInstallation

    Write-Host ""

    # Environment setup
    if (-not (New-VirtualEnvironment -PythonCmd $pythonCmd)) {
        exit 1
    }

    if (-not (Enable-VirtualEnvironment)) {
        exit 1
    }

    Write-Host ""

    # Dependency installation
    if (-not (Update-Pip)) {
        exit 1
    }

    if (-not (Install-Dependencies)) {
        exit 1
    }

    Write-Host ""

    # Verification
    Test-Installation

    # Summary and final message
    Write-Summary -StartTime $startTime
    Write-FinalMessage
}

# ==============================================================================
# SCRIPT ENTRY POINT
# ==============================================================================

try {
    Main
}
catch {
    Write-Host ""
    Write-ErrorMsg "Setup failed: $_"
    Write-Info "Check the error messages above for details"
    exit 1
}
