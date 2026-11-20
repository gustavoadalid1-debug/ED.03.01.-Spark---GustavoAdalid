import os
from comun import crear_spark, ruta_local, guardar_rdd

# Paso 1: configuracion y carga
spark = crear_spark("AnalisisStocks", "512m")
sc = spark.sparkContext

archivo = os.path.join("DATOS", "stocks.txt")
if not os.path.exists(archivo):
    raise SystemExit("Falta DATOS/stocks.txt. Ejecuta: python descargar_datos.py")

rdd = sc.textFile(ruta_local(archivo))
print("Primeras 5 lineas:")
print(rdd.take(5))


# Paso 2: limpieza y parseo
encabezado = rdd.first()
columnas = [c.strip().lower() for c in encabezado.split(",")]
alias = {
    "simbolo": ["simbolo", "symbol", "name"],
    "fecha": ["fecha", "date", "category"],
    "precio": ["precio", "price"],
    "volumen": ["volumen", "volume", "quantity"],
}
pos = {}
for campo, nombres in alias.items():
    for n in nombres:
        if n in columnas:
            pos[campo] = columnas.index(n)
            break
if len(pos) < 4:
    raise SystemExit(f"Formato de archivo no reconocido: {encabezado}")
print(f"Columnas detectadas: {pos}")


def parsear_linea(linea):
    try:
        partes = linea.split(",")
        simbolo = partes[pos["simbolo"]].strip()
        fecha = partes[pos["fecha"]].strip()
        precio = float(partes[pos["precio"]])
        volumen = int(partes[pos["volumen"]])
        return (simbolo, fecha, precio, volumen)
    except Exception:
        return None


rdd_parseado = rdd.map(parsear_linea).filter(lambda x: x is not None).cache()
validas = rdd_parseado.count()
print(f"Lineas validas: {validas}")
if validas == 0:
    raise SystemExit("No hay lineas validas. Revisa el formato de DATOS/stocks.txt")

# Paso 3: estadisticas por simbolo
simbolo_precio = rdd_parseado.map(lambda x: (x[0], x[2]))

precio_max = simbolo_precio.reduceByKey(max)
precio_min = simbolo_precio.reduceByKey(min)

precio_promedio = simbolo_precio.aggregateByKey(
    (0.0, 0),
    lambda acc, valor: (acc[0] + valor, acc[1] + 1),
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).mapValues(lambda x: x[0] / x[1])

print("\nPrecio maximo por simbolo:")
for simbolo, precio in sorted(precio_max.collect()):
    print(f"{simbolo}: ${precio:.2f}")

print("\nPrecio minimo por simbolo:")
for simbolo, precio in sorted(precio_min.collect()):
    print(f"{simbolo}: ${precio:.2f}")

print("\nPrecio promedio por simbolo:")
for simbolo, precio in sorted(precio_promedio.collect()):
    print(f"{simbolo}: ${precio:.2f}")

# Paso 4: top dias por volumen
fecha_volumen = rdd_parseado.map(lambda x: (x[1], x[3]))
volumen_por_fecha = fecha_volumen.reduceByKey(lambda a, b: a + b)
top_fechas = volumen_por_fecha.sortBy(lambda x: x[1], ascending=False)

print("\nTop 10 dias con mayor volumen:")
for fecha, volumen in top_fechas.take(10):
    print(f"{fecha}: {volumen:,} acciones")

# Paso 5: volatilidad
stats_por_simbolo = simbolo_precio.aggregateByKey(
    (float("-inf"), float("inf")),
    lambda acc, precio: (max(acc[0], precio), min(acc[1], precio)),
    lambda a, b: (max(a[0], b[0]), min(a[1], b[1]))
)

volatilidad = stats_por_simbolo.mapValues(lambda x: ((x[0] - x[1]) / x[1]) * 100)

mas_volatiles = volatilidad.sortBy(lambda x: x[1], ascending=False).take(5)
mas_estables = volatilidad.sortBy(lambda x: x[1], ascending=True).take(5)

print("\nSimbolos mas volatiles:")
for simbolo, vol in mas_volatiles:
    print(f"{simbolo}: {vol:.2f}%")

print("\nSimbolos mas estables:")
for simbolo, vol in mas_estables:
    print(f"{simbolo}: {vol:.2f}%")

# Paso 6: resultados y resumen
os.makedirs("RESULTADOS", exist_ok=True)
guardar_rdd(volumen_por_fecha, os.path.join("RESULTADOS", "volumen_por_fecha"))
guardar_rdd(stats_por_simbolo, os.path.join("RESULTADOS", "stats_por_simbolo"))

total_registros = rdd_parseado.count()
simbolos_unicos = rdd_parseado.map(lambda x: x[0]).distinct().count()
simbolo_mayor_precio = precio_promedio.sortBy(lambda x: x[1], ascending=False).first()
dia_mayor_volumen = volumen_por_fecha.sortBy(lambda x: x[1], ascending=False).first()
simbolo_mas_volatil = volatilidad.sortBy(lambda x: x[1], ascending=False).first()

resumen = []
resumen.append("RESUMEN ANALISIS DE STOCKS")
resumen.append(f"Total de registros procesados: {total_registros:,}")
resumen.append(f"Numero de simbolos unicos: {simbolos_unicos}")
resumen.append(f"Simbolo con mayor precio promedio: {simbolo_mayor_precio[0]} (${simbolo_mayor_precio[1]:.2f})")
resumen.append(f"Dia con mayor volumen: {dia_mayor_volumen[0]} ({dia_mayor_volumen[1]:,} acciones)")
resumen.append(f"Simbolo mas volatil: {simbolo_mas_volatil[0]} ({simbolo_mas_volatil[1]:.2f}%)")
texto = "\n".join(resumen)

print("\n" + texto)
with open(os.path.join("RESULTADOS", "resumen_ejecutivo.txt"), "w", encoding="utf-8") as f:
    f.write(texto + "\n")

spark.stop()
print("\nAnalisis completado. Sesion cerrada.")
