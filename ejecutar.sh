#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

PY=python3
for v in python3.12 python3.11 python3.10; do
    if command -v $v >/dev/null 2>&1; then PY=$v; break; fi
done
if [ ! -d ".venv" ]; then
    $PY -m venv .venv
fi
source .venv/bin/activate
pip install -q -r requirements.txt

python descargar_datos.py
python analisis_stocks.py
