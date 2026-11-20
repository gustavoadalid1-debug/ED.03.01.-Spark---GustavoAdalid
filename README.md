# ED.03.01 - Spark Standalone

Analisis de acciones bursatiles con Apache Spark (PySpark) usando RDDs en modo local. Autor: Gustavo Adalid Catalan Carmen.

## Contenido

| Archivo | Descripcion |
|---|---|
| `analisis_stocks.py` | Proyecto principal, pasos 1 a 6 |
| `ejercicios.py` | Ejercicios practicos 1 a 7 |
| `comun.py` | Funciones compartidas (sesion Spark, rutas, guardado) |
| `descargar_datos.py` | Descarga los datasets a la carpeta `DATOS` |
| `ejecutar.sh` | Ejecucion automatica en Linux |
| `ejecutar.bat` | Ejecucion automatica en Windows |
| `respuestas.md` | Respuestas a las preguntas de reflexion |
| `requirements.txt` | Dependencias de Python |

## Requisitos

- Python 3.10, 3.11 o 3.12 (no usar 3.13 ni 3.14, PySpark 3.5 no los soporta)
- Java 17 o 21 (JDK). Spark 3.5 no funciona con Java 22 o mayor
- Conexion a internet la primera vez, para instalar PySpark y descargar los datos

## Fedora Linux

1. Instalar Python y Java:

```bash
sudo dnf install -y python3.12 git java-21-openjdk-devel
```

2. Verificar Java y Python 3.12:

```bash
java -version
python3.12 --version
```

Fedora puede traer un Java mas nuevo. No hace falta desinstalarlo, el programa busca y usa el JDK 17 o 21 solo. Si quieres dejarlo como predeterminado:

```bash
sudo alternatives --config java
```

3. Clonar el repositorio:

```bash
git clone https://github.com/gustavoadalid1-debug/ED.03.01.-Spark---GustavoAdalid.git
cd ED.03.01.-Spark---GustavoAdalid
```

4. Ejecutar el proyecto de forma automatica:

```bash
chmod +x ejecutar.sh
./ejecutar.sh
```

Ejecucion manual, paso a paso:

```bash
rm -rf .venv
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python descargar_datos.py
python analisis_stocks.py
python ejercicios.py
```

## Windows 11

1. Instalar Python desde https://www.python.org/downloads/ y marcar la opcion "Add python.exe to PATH".
2. Instalar Java 17 con PowerShell:

```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```

3. Cerrar y abrir de nuevo PowerShell, y verificar:

```powershell
java -version
python --version
```

4. Clonar el repositorio:

```powershell
git clone https://github.com/gustavoadalid1-debug/ED.03.01.-Spark---GustavoAdalid.git
cd ED.03.01.-Spark---GustavoAdalid
```

5. Ejecutar de forma automatica, con doble clic en `ejecutar.bat` o desde PowerShell:

```powershell
.\ejecutar.bat
```

Ejecucion manual, paso a paso:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python descargar_datos.py
python analisis_stocks.py
python ejercicios.py
```

Si PowerShell bloquea la activacion del entorno virtual, ejecutar una vez:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

## Formato del dataset

El programa lee el encabezado de `DATOS/stocks.txt` y detecta las columnas. Acepta `Simbolo,Fecha,Precio,Volumen` y tambien el archivo de productos `id,name,category,quantity,price`. En este ultimo caso, `name` se trata como simbolo, `category` como fecha, `price` como precio y `quantity` como volumen.

## Resultados

Al terminar, el programa muestra en consola las estadisticas y el resumen ejecutivo, y crea la carpeta `RESULTADOS` con:

- `volumen_por_fecha`
- `stats_por_simbolo`
- `resumen_ejecutivo.txt`

Mientras corre, la interfaz Spark UI esta en http://localhost:4040.

## Problemas comunes

| Error | Solucion |
|---|---|
| `getSubject is not supported` | Java demasiado nuevo. Instalar JDK 21 (`sudo dnf install java-21-openjdk-devel`) |
| `JAVA_GATEWAY_EXITED` o `java: No such file` | Java no esta instalado o `JAVA_HOME` apunta a una ruta que no existe. Ejecutar `unset JAVA_HOME` (Linux) y confirmar `java -version`. El programa detecta Java solo |
| `Falta DATOS/stocks.txt` | Ejecutar `python descargar_datos.py` |
| `Port 4040 is already in use` | Spark usa el siguiente puerto disponible, es solo un aviso |
| `Python worker failed to connect` en Windows | Usar el entorno virtual del proyecto, ya que `comun.py` fija el Python correcto |
| Error al guardar resultados en Windows | El programa usa un respaldo que escribe los archivos con Python, no requiere configuracion extra |
