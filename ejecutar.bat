@echo off
cd /d "%~dp0"

if not exist .venv (
    py -3 -m venv .venv
)
call .venv\Scripts\activate.bat
pip install -q -r requirements.txt

python descargar_datos.py
python analisis_stocks.py
pause
