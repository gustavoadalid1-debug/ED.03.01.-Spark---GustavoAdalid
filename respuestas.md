# Preguntas de reflexion

1. **Eficiencia.** Cada `split()` recorre la linea completa y crea una lista nueva. Hacerlo una sola vez evita repetir ese trabajo por cada campo. Con 1 millon de lineas y 3 campos, se pasa de 3 millones de splits a 1 millon, es decir, cerca de dos tercios menos de trabajo de parseo.
2. **Particionamiento.** Se consulta con `rdd.getNumPartitions()`. Cada particion se procesa en una tarea, asi que mas particiones permiten mas paralelismo hasta llegar al numero de cores. Con una sola particion todo corre en una tarea y no hay paralelismo.
3. **Lazy evaluation.** Las transformaciones (`map`, `filter`) solo construyen un plan. Se ejecutan al llamar una accion como `count`, `take` o `collect`. Esto permite a Spark optimizar el plan completo y evitar calculos que nunca se usan.
4. **Memoria.** Un `collect()` sobre 10 GB intenta traer todo al driver y puede causar `OutOfMemoryError`. Se evita con `take(n)`, agregando antes de traer datos, o guardando con `saveAsTextFile`.
5. **Optimizacion.** Con 100 veces mas datos conviene usar mas particiones, cachear con `cache()` los RDD que se reutilizan, aumentar la memoria de los executors y migrar a DataFrames.
6. **HDFS vs local.** HDFS distribuye y replica los datos entre nodos, tolera fallas, permite que cada executor lea datos cercanos y escala a volumenes que no caben en una sola maquina.
