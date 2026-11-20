import os
import urllib.request

BASE = "https://raw.githubusercontent.com/josepgarcia/datos/refs/heads/main/"
ARCHIVOS = ["stocks.txt", "persons.csv", "pdi_sales_small.csv"]


def descargar():
    os.makedirs("DATOS", exist_ok=True)
    for nombre in ARCHIVOS:
        destino = os.path.join("DATOS", nombre)
        if os.path.exists(destino):
            print(f"Ya existe: {destino}")
            continue
        try:
            urllib.request.urlretrieve(BASE + nombre, destino)
            print(f"Descargado: {destino}")
        except Exception as e:
            print(f"No se pudo descargar {nombre}: {e}")


if __name__ == "__main__":
    descargar()
