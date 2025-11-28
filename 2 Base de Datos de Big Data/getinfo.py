from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, isnan, when, desc, avg, min, max

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("EDA_Vuelos") \
    .getOrCreate()

# Leer el CSV
df = spark.read.csv("Airline_Delay_2016-2018.csv", header=True, inferSchema=True)

# Mostrar esquema de datos
df.printSchema()

# Número total de registros
total_registros = df.count()
print(f"Número total de registros: {total_registros}")

# Lista de columnas
columnas = df.columns

# Mapear tipos de datos
tipo_columnas = dict(df.dtypes)

# Conteo de CANCELLED
print("=== Conteo de CANCELLED ===")
df.groupBy("CANCELLED").count().show()

# Conteo de DIVERTED
print("=== Conteo de DIVERTED ===")
df.groupBy("DIVERTED").count().show()

# Analizar cada columna
for columna in columnas:
    print(f"\n=== Análisis de columna: {columna} ===")
    
    tipo_dato = tipo_columnas[columna]
    
    if tipo_dato in ['double', 'float']:
        # Nulos y NaNs para numéricas flotantes
        nulos = df.filter((col(columna).isNull()) | (isnan(col(columna)))).count()
    else:
        # Solo nulos (y vacíos para strings) para otros tipos
        nulos = df.filter((col(columna).isNull()) | (col(columna) == "")).count()
    
    print(f"Valores nulos o vacíos: {nulos}")
    
    # Número de valores únicos
    distintos = df.select(columna).distinct().count()
    print(f"Valores únicos: {distintos}")
    
    # Análisis según tipo de dato
    if tipo_dato in ['int', 'bigint', 'double', 'float']:
        resumen = df.select(
            min(columna).alias("Minimo"),
            max(columna).alias("Maximo"),
            avg(columna).alias("Promedio")
        ).collect()[0]
        print(f"Min: {resumen['Minimo']}, Max: {resumen['Maximo']}, Promedio: {resumen['Promedio']:.2f}")
    
    elif tipo_dato in ['string', 'date', 'timestamp']:
        print("Top 5 valores más frecuentes:")
        top5 = df.groupBy(columna).count().orderBy(desc("count")).limit(5)
        top5.show(truncate=False)

# Cerrar sesión Spark al terminar
spark.stop()
