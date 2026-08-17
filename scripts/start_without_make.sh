#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$PROJECT_ROOT/.venv"

# Ensure virtualenv exists; create if missing
if [ ! -d "$VENV" ]; then
  echo "Virtual environment not found at $VENV. Creating one..."
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is not installed or not on PATH. Please install Python 3 and re-run this script."
    exit 1
  fi
  python3 -m venv "$VENV"
  echo "Created virtual environment at $VENV"
fi

# Activate the virtual environment
# shellcheck disable=SC1091
. "$VENV/bin/activate"

# Upgrade packaging tools and install requirements if present
echo "Upgrading pip, setuptools, and wheel in virtualenv..."
pip install --upgrade pip setuptools wheel
if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
  echo "Installing Python requirements from requirements.txt..."
  pip install -r "$PROJECT_ROOT/requirements.txt"
fi

# Ensure .env exists
if [ ! -f "$PROJECT_ROOT/.env" ]; then
  if [ -f "$PROJECT_ROOT/.env.example" ]; then
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
    echo "Created .env from .env.example"
  else
    echo "Warning: .env not found and .env.example missing. Aborting."
    exit 1
  fi
fi

# Start Docker Compose
echo "Bringing up Docker Compose services..."
docker compose up -d --build

echo "Services started. Use 'docker compose ps' to check status and 'docker compose logs -f <service>' to follow logs."
