#!/bin/bash
set -e

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   🌈  Assemblief Trader  — Live Edition  ║"
echo "╚══════════════════════════════════════════╝"
echo ""

cd "$(dirname "$0")/assemblief-main"

if ! command -v python3 &>/dev/null; then
  echo "❌  Python 3 not found. Install it from https://python.org then rerun."
  exit 1
fi

PY=$(command -v python3)
echo "✔  Python: $($PY --version)"

if [ -d ".venv" ]; then
  echo "⚙  Removing old virtual environment..."
  rm -rf .venv
fi

echo "⚙  Creating virtual environment..."
$PY -m venv .venv
source .venv/bin/activate

echo "📦  Installing dependencies..."
pip install -q -r requirements.txt

echo ""
echo "🚀  Starting server → http://127.0.0.1:8000"
echo "    (browser will open automatically)"
echo "    Press Ctrl+C to stop."
echo ""

python main.py
