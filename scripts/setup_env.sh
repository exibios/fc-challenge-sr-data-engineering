#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
REQ_FILE="$PROJECT_ROOT/requirements.txt"

echo "=== Project bootstrap: create virtualenv and install Python deps ==="

if [ ! -x "$(command -v python3)" ]; then
    echo "ERROR: python3 is required but not found in PATH. Install Python 3 and retry." >&2
    exit 1
fi

echo "Creating virtual environment at $VENV_DIR..."
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1091
. "$VENV_DIR/bin/activate"

echo "Upgrading pip and build tools..."
pip install --upgrade pip setuptools wheel

if [ -f "$REQ_FILE" ]; then
    echo "Installing Python dependencies from requirements.txt..."
    pip install -r "$REQ_FILE"
else
    echo "No requirements.txt found; skipping pip install."
fi

# Ensure .env exists (copy from .env.example)
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    if [ -f "$PROJECT_ROOT/.env.example" ]; then
        cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
        echo "Created .env from .env.example"
    else
        echo "No .env.example found — create .env manually."
    fi
else
    echo ".env already exists — leaving it alone."
fi

echo
echo "Optional: ensure 'make' is available for project commands."
if command -v make >/dev/null 2>&1; then
    echo "make is already installed."
else
    echo "make not found. Attempt to install it automatically?"
    read -p "Install make using package manager (may require sudo)? (y/N) " install_make
    if [ "${install_make,,}" = "y" ]; then
        OS_TYPE="$(uname -s)"
        if command -v apt-get >/dev/null 2>&1; then
            echo "Detected apt-get. Installing make via sudo apt-get..."
            sudo apt-get update && sudo apt-get install -y make build-essential
        elif command -v yum >/dev/null 2>&1; then
            echo "Detected yum. Installing make via sudo yum..."
            sudo yum install -y make gcc gcc-c++
        elif command -v apk >/dev/null 2>&1; then
            echo "Detected apk. Installing make via sudo apk..."
            sudo apk add make build-base
        elif [ "$OS_TYPE" = "Darwin" ]; then
            if command -v brew >/dev/null 2>&1; then
                echo "Detected Homebrew. Installing make via brew..."
                brew install make
            else
                echo "Homebrew not found. On macOS, install Xcode command line tools or Homebrew first."
                echo "You can run: xcode-select --install  OR install Homebrew from https://brew.sh/ and then 'brew install make'"
            fi
        else
            echo "Could not detect a supported package manager. Please install 'make' manually."
        fi
    else
        echo "Skipping automatic make installation. You can install 'make' later if needed." 
    fi
fi

echo
echo "Bootstrap complete. Activate the environment with:"
echo "  source $VENV_DIR/bin/activate"

echo "You can now run 'make help' to see available project commands."
