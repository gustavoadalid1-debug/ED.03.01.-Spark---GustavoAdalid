import os
from comun import crear_spark, ruta_local

# Ejercicio 1
spark = crear_spark("MiPrimerAnalisis", "1g")
spark.conf.set("spark.sql.shuffle.partitions", "8")
sc = spark.sparkContext
print("Ejercicio 1:", spark.version, spark.sparkContext.appName,
      spark.conf.get("spark.sql.shuffle.partitions"))

# Ejercicio 2
rdd = sc.parallelize(list(range(1, 21)))
print("\nEjercicio 2")
print("Total:", rdd.count())
print("Primeros 5:", rdd.take(5))
print("Suma:", rdd.sum())
print("Maximo y minimo:", rdd.max(), rdd.min())

# Ejercicio 3
numeros = sc.parallelize(range(1, 51))
triples = numeros.map(lambda x: x * 3)
mayores = triples.filter(lambda x: x > 50)
print("\nEjercicio 3")
print("Total mayores a 50:", mayores.count())
print("Primeros 10:", mayores.take(10))

# Ejercicio 4
ventas = [("Laptop", 1200), ("Mouse", 25), ("Laptop", 1100),
          ("Teclado", 75), ("Mouse", 30), ("Laptop", 1300)]
rdd_ventas = sc.parallelize(ventas)
totales = rdd_ventas.reduceByKey(lambda a, b: a + b)
conteo = rdd_ventas.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
print("\nEjercicio 4")
print("Total por producto:", sorted(totales.collect()))
print("Numero de ventas:", sorted(conteo.collect()))
print("Mayor total:", totales.sortBy(lambda x: x[1], ascending=False).first())

# Ejercicio 5
print("\nEjercicio 5")
archivo = os.path.join("DATOS", "persons.csv")
if os.path.exists(archivo):
    lineas = sc.textFile(ruta_local(archivo))
    encabezado = lineas.first()
    datos = lineas.filter(lambda l: l != encabezado)
    personas = datos.map(lambda l: l.split(",")).map(lambda p: (p[0], int(p[1])))
    mayores_30 = personas.filter(lambda p: p[1] > 30)
    print("Mayores de 30:", mayores_30.collect())
    print("Total:", mayores_30.count())
else:
    print("Falta DATOS/persons.csv. Ejecuta: python descargar_datos.py")


# Ejercicio 6
def parsear(linea):
    partes = linea.split(",")
    return (partes[0], int(partes[1]), partes[2])


print("\nEjercicio 6")
muestra = sc.parallelize(["Ana,28,Madrid", "Juan,35,Barcelona", "Maria,42,Valencia"])
print(muestra.map(parsear).collect())

# Ejercicio 7
print("\nEjercicio 7")
pares = sc.parallelize(range(1000000), 10)
print("Particiones:", pares.getNumPartitions())
print("Pares:", pares.filter(lambda x: x % 2 == 0).count())

spark.stop()
