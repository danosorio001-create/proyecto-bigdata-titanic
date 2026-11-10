from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt

# 1. Inicializar Spark limitando el driver a 2 GB de RAM para cuidar los recursos de tu Mac
spark = SparkSession.builder \
    .appName("ProyectoBatchBigData") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("--- Sesión de Spark iniciada correctamente ---")

# 2. Cargar el dataset del Titanic
df = spark.read.option("header", "true").option("inferSchema", "true").csv("datos_prueba.csv")

# 3. Mostrar la estructura y las primeras filas
print("--- Estructura del Dataset del Titanic ---")
df.printSchema()
df.show(5)

# 4. Limpieza y Transformación (Agrupamos por la clase de pasajero 'Pclass')
df_clean = df.dropna()
resumen_df = df_clean.groupBy("Pclass").agg(count("*").alias("total_pasajeros"))
resumen_df.show()

# 5. Visualización de resultados con Pandas y Matplotlib
pdf = resumen_df.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(pdf["Pclass"].astype(str), pdf["total_pasajeros"], color='salmon')
plt.xlabel("Clase de Pasajero (Pclass)")
plt.ylabel("Total")
plt.title("Distribución de Pasajeros por Clase - Titanic")
plt.tight_layout()
plt.savefig("resultado_batch.png")
print("¡Gráfico guardado exitosamente como resultado_batch.png!")

spark.stop()
