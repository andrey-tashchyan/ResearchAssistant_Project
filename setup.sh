#!/usr/bin/env bash
# ==============================================================================
# PSID Pipeline - Environment Setup Script
# ==============================================================================
# Purpose: Automated virtual environment creation and dependency installation
#          for the PSID data processing pipeline (academic research tool)
#
# Usage:   chmod +x setup.sh && ./setup.sh
#
# Features:
#   - Creates Python virtual environment (psid_env)
#   - Installs all required dependencies
#   - Color-coded progress output
#   - Progress indicators for long operations
#   - macOS and Linux compatible
#   - Safe error handling and validation
#
# Author:  PSID Pipeline Team
# Date:    2024-11-01
# ==============================================================================

set -euo pipefail  # Exit on error, undefined variables, pipe failures

# ==============================================================================
# CONFIGURATION
# ==============================================================================

VENV_NAME="psid_env"
REQUIREMENTS_FILE="requirements.txt"
PYTHON_MIN_VERSION="3.8"

# Default dependencies if requirements.txt doesn't exist
DEFAULT_PACKAGES=(
    "pandas>=1.5.0"
    "numpy>=1.23.0"
    "pyarrow>=10.0.0"
    "openpyxl>=3.0.0"
    "tqdm>=4.65.0"
    "regex>=2022.10.31"
    "multiprocess>=0.70.14"
)

# ==============================================================================
# COLOR CODES FOR TERMINAL OUTPUT
# ==============================================================================

# ANSI color codes
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly MAGENTA='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly DIM='\033[2m'
readonly RESET='\033[0m'

# Unicode symbols (fallback for compatibility)
readonly CHECK_MARK="✓"
readonly CROSS_MARK="✗"
readonly ARROW="→"
readonly SPINNER_FRAMES=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

# Print functions with color
print_header() {
    echo -e "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${BOLD}${CYAN}  $1${RESET}"
    echo -e "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}\n"
}

print_step() {
    echo -e "${BLUE}${ARROW}${RESET} ${BOLD}$1${RESET}"
}

print_success() {
    echo -e "${GREEN}${CHECK_MARK}${RESET} $1"
}

print_skip() {
    echo -e "${YELLOW}⊘${RESET} $1"
}

print_error() {
    echo -e "${RED}${CROSS_MARK}${RESET} ${BOLD}ERROR:${RESET} $1" >&2
}

print_info() {
    echo -e "${DIM}  ℹ $1${RESET}"
}

print_warning() {
    echo -e "${YELLOW}⚠${RESET}  $1"
}

# Show spinner for background processes
show_spinner() {
    local pid=$1
    local message=$2
    local i=0

    # Hide cursor
    tput civis 2>/dev/null || true

    while kill -0 "$pid" 2>/dev/null; do
        printf "\r${BLUE}${SPINNER_FRAMES[$i]}${RESET} ${message}..."
        i=$(( (i + 1) % ${#SPINNER_FRAMES[@]} ))
        sleep 0.1
    done

    # Show cursor
    tput cnorm 2>/dev/null || true

    # Clear spinner line
    printf "\r\033[K"
}

# Progress bar for operations with known duration
show_progress_bar() {
    local current=$1
    local total=$2
    local width=50
    local percentage=$((current * 100 / total))
    local completed=$((width * current / total))

    printf "\r${CYAN}["
    printf "%${completed}s" | tr ' ' '█'
    printf "%$((width - completed))s" | tr ' ' '░'
    printf "]${RESET} ${BOLD}%d%%${RESET}" "$percentage"
}

# ==============================================================================
# VALIDATION FUNCTIONS
# ==============================================================================

# Check if Python is installed and meets minimum version
check_python() {
    print_step "Checking Python installation..."

    # Try to find Python 3
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        print_error "Python is not installed or not in PATH"
        print_info "Please install Python ${PYTHON_MIN_VERSION}+ from https://www.python.org/"
        exit 1
    fi

    # Get Python version
    PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
    PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

    # Validate version
    MIN_MAJOR=$(echo "$PYTHON_MIN_VERSION" | cut -d. -f1)
    MIN_MINOR=$(echo "$PYTHON_MIN_VERSION" | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -lt "$MIN_MAJOR" ] || \
       { [ "$PYTHON_MAJOR" -eq "$MIN_MAJOR" ] && [ "$PYTHON_MINOR" -lt "$MIN_MINOR" ]; }; then
        print_error "Python ${PYTHON_MIN_VERSION}+ required (found ${PYTHON_VERSION})"
        exit 1
    fi

    print_success "Found Python ${PYTHON_VERSION} at $(command -v $PYTHON_CMD)"
}

# Check OS compatibility
check_os() {
    print_step "Checking operating system..."

    OS_TYPE=$(uname -s)
    case "$OS_TYPE" in
        Darwin)
            print_success "macOS detected"
            ;;
        Linux)
            print_success "Linux detected"
            ;;
        *)
            print_warning "Untested OS: $OS_TYPE (script may still work)"
            ;;
    esac
}

# ==============================================================================
# VIRTUAL ENVIRONMENT FUNCTIONS
# ==============================================================================

# Create virtual environment
create_venv() {
    if [ -d "$VENV_NAME" ]; then
        print_skip "Virtual environment '${VENV_NAME}' already exists"
        return 0
    fi

    print_step "Creating virtual environment '${VENV_NAME}'..."

    # Create venv in background with spinner
    $PYTHON_CMD -m venv "$VENV_NAME" &
    local pid=$!
    show_spinner $pid "Creating virtual environment"

    # Wait for completion and check exit status
    wait $pid
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        print_success "Virtual environment created: ${VENV_NAME}/"
    else
        print_error "Failed to create virtual environment (exit code: $exit_code)"
        exit 1
    fi
}

# Activate virtual environment
activate_venv() {
    print_step "Activating virtual environment..."

    # Check activation script exists
    ACTIVATE_SCRIPT="${VENV_NAME}/bin/activate"
    if [ ! -f "$ACTIVATE_SCRIPT" ]; then
        print_error "Activation script not found: $ACTIVATE_SCRIPT"
        exit 1
    fi

    # Source the activation script
    # shellcheck disable=SC1090
    source "$ACTIVATE_SCRIPT"

    # Verify activation
    if [ -n "${VIRTUAL_ENV:-}" ]; then
        print_success "Virtual environment activated"
        print_info "Active Python: $(which python)"
    else
        print_error "Failed to activate virtual environment"
        exit 1
    fi
}

# ==============================================================================
# DEPENDENCY INSTALLATION
# ==============================================================================

# Upgrade pip to latest version
upgrade_pip() {
    print_step "Upgrading pip..."

    # Upgrade pip in background
    python -m pip install --upgrade pip --quiet &
    local pid=$!
    show_spinner $pid "Upgrading pip"

    wait $pid
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        local pip_version=$(pip --version | awk '{print $2}')
        print_success "pip upgraded to version ${pip_version}"
    else
        print_error "Failed to upgrade pip (exit code: $exit_code)"
        exit 1
    fi
}

# Create default requirements.txt if it doesn't exist
create_default_requirements() {
    print_step "Creating default requirements.txt..."

    {
        echo "# PSID Pipeline - Python Dependencies"
        echo "# Generated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        echo "# Core data processing"
        for package in "${DEFAULT_PACKAGES[@]}"; do
            echo "$package"
        done
    } > "$REQUIREMENTS_FILE"

    print_success "Created ${REQUIREMENTS_FILE} with default packages"
    print_info "File location: $(pwd)/${REQUIREMENTS_FILE}"
}

# Install dependencies from requirements.txt
install_dependencies() {
    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        print_warning "requirements.txt not found"
        create_default_requirements
    else
        print_step "Found existing ${REQUIREMENTS_FILE}"
    fi

    # Count number of packages
    local package_count=$(grep -c '^[^#]' "$REQUIREMENTS_FILE" | grep -v '^$' || echo "0")

    print_step "Installing dependencies from ${REQUIREMENTS_FILE}..."
    print_info "Installing ${package_count} package(s)..."

    # Install with progress output
    if command -v tqdm &> /dev/null || grep -q "tqdm" "$REQUIREMENTS_FILE"; then
        # If tqdm is available or will be installed, use it
        python -m pip install -r "$REQUIREMENTS_FILE" --progress-bar on
    else
        # Install in background with spinner
        python -m pip install -r "$REQUIREMENTS_FILE" --quiet &
        local pid=$!
        show_spinner $pid "Installing packages"
        wait $pid
    fi

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        print_success "All dependencies installed successfully"

        # Show installed packages
        print_info "Installed packages:"
        while IFS= read -r package; do
            # Skip comments and empty lines
            if [[ $package =~ ^[^#].+ ]]; then
                local pkg_name=$(echo "$package" | cut -d'>' -f1 | cut -d'=' -f1 | cut -d'<' -f1 | cut -d'[' -f1)
                local pkg_version=$(pip show "$pkg_name" 2>/dev/null | grep "Version:" | awk '{print $2}')
                if [ -n "$pkg_version" ]; then
                    echo -e "  ${DIM}•${RESET} ${pkg_name} ${GREEN}${pkg_version}${RESET}"
                fi
            fi
        done < "$REQUIREMENTS_FILE"
    else
        print_error "Failed to install dependencies (exit code: $exit_code)"
        print_info "Try running: pip install -r ${REQUIREMENTS_FILE}"
        exit 1
    fi
}

# ==============================================================================
# VERIFICATION
# ==============================================================================

# Verify installation
verify_installation() {
    print_step "Verifying installation..."

    # Test imports of critical packages
    local critical_packages=("pandas" "numpy" "pyarrow")
    local all_ok=true

    for package in "${critical_packages[@]}"; do
        if python -c "import ${package}" 2>/dev/null; then
            local version=$(python -c "import ${package}; print(${package}.__version__)" 2>/dev/null || echo "unknown")
            echo -e "  ${GREEN}${CHECK_MARK}${RESET} ${package} ${DIM}(${version})${RESET}"
        else
            echo -e "  ${RED}${CROSS_MARK}${RESET} ${package} ${DIM}(import failed)${RESET}"
            all_ok=false
        fi
    done

    if [ "$all_ok" = true ]; then
        print_success "All critical packages verified"
    else
        print_warning "Some packages failed verification (may still work)"
    fi
}

# ==============================================================================
# CLEANUP AND FINAL MESSAGES
# ==============================================================================

# Print final success message
print_final_message() {
    echo ""
    echo -e "${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${BOLD}${GREEN}  ✅ PSID environment ready${RESET}"
    echo -e "${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""
    echo -e "${BOLD}To activate the environment, run:${RESET}"
    echo -e "  ${CYAN}source ${VENV_NAME}/bin/activate${RESET}"
    echo ""
    echo -e "${BOLD}To deactivate when done:${RESET}"
    echo -e "  ${CYAN}deactivate${RESET}"
    echo ""
    echo -e "${BOLD}Next steps:${RESET}"
    echo -e "  ${DIM}1.${RESET} Run the pipeline: ${CYAN}./run_all.sh${RESET}"
    echo -e "  ${DIM}2.${RESET} Or run individual scripts (see README.md)"
    echo ""
    echo -e "${DIM}Project location: $(pwd)${RESET}"
    echo ""
}

# Print summary statistics
print_summary() {
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    echo ""
    echo -e "${BOLD}Setup Summary:${RESET}"
    echo -e "  ${DIM}•${RESET} Virtual environment: ${GREEN}${VENV_NAME}/${RESET}"
    echo -e "  ${DIM}•${RESET} Python version: ${GREEN}${PYTHON_VERSION}${RESET}"
    echo -e "  ${DIM}•${RESET} Dependencies: ${GREEN}$(pip list --format=freeze | wc -l | xargs)${RESET} packages installed"
    echo -e "  ${DIM}•${RESET} Time elapsed: ${GREEN}${elapsed}s${RESET}"
}

# ==============================================================================
# ERROR HANDLING
# ==============================================================================

# Cleanup on error
cleanup_on_error() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo ""
        print_error "Setup failed with exit code $exit_code"
        print_info "Check the error messages above for details"

        # Offer to clean up partial installation
        if [ -d "$VENV_NAME" ]; then
            echo ""
            read -p "Remove incomplete virtual environment? [y/N] " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                rm -rf "$VENV_NAME"
                print_success "Removed ${VENV_NAME}/"
            fi
        fi
    fi
}

# Register cleanup handler
trap cleanup_on_error EXIT

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

main() {
    local start_time=$(date +%s)

    # Print banner
    print_header "🚀 PSID Pipeline - Environment Setup"

    echo -e "${BOLD}This script will:${RESET}"
    echo -e "  ${DIM}1.${RESET} Validate Python installation"
    echo -e "  ${DIM}2.${RESET} Create virtual environment (${VENV_NAME})"
    echo -e "  ${DIM}3.${RESET} Install/upgrade pip"
    echo -e "  ${DIM}4.${RESET} Install project dependencies"
    echo -e "  ${DIM}5.${RESET} Verify installation"
    echo ""

    # Pre-flight checks
    check_os
    check_python

    echo ""

    # Environment setup
    create_venv
    activate_venv

    echo ""

    # Dependency installation
    upgrade_pip
    install_dependencies

    echo ""

    # Verification
    verify_installation

    # Summary and final message
    print_summary
    print_final_message

    # Unset error trap since we succeeded
    trap - EXIT
}

# ==============================================================================
# SCRIPT ENTRY POINT
# ==============================================================================

# Only run main if script is executed (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
