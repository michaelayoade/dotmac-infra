#!/usr/bin/env bash
set -euo pipefail

# Ensure local installable and dev tools available
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
fi

# Install dev tools just in case they are not in requirements
pip install --upgrade pip setuptools wheel
pip install pytest pytest-cov mypy

# Type-check
echo "Running MyPy..."
mypy dotmac_infra || { echo "MyPy failed"; exit 1; }

# Run tests
echo "Running pytest..."
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -q --maxfail=1 --disable-warnings

echo "All checks passed."