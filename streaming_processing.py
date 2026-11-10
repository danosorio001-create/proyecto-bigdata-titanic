from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

# 1. Inicializar la sesión de Spark para Streaming (recurso optimizado a 2GB)
spark = SparkSession.builder \
    .appName("TitanicStreamingRealTime") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema exacto del dataset del Titanic
schema = StructType() \
    .add("PassengerId", IntegerType(), True) \
    .add("Survived", IntegerType(), True) \
    .add("Pclass", IntegerType(), True) \
    .add("Name", StringType(), True) \
    .add("Sex", StringType(), True) \
    .add("Age", DoubleType(), True) \
    .add("SibSp", IntegerType(), True) \
    .add("Parch", IntegerType(), True) \
    .add("Ticket", StringType(), True) \
    .add("Fare", DoubleType(), True) \
    .add("Cabin", StringType(), True) \
    .add("Embarked", StringType(), True)

print("--- Iniciando el motor de Streaming de PySpark ---")

# 3. Leer los datos en tiempo real desde la carpeta stream_input
streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("stream_input")

# 4. Transformación en tiempo real: Filtrar solo los pasajeros de Primera Clase (Pclass == 1)
filtered_stream = streaming_df.filter(col("Pclass") == 1) \
    .select("PassengerId", "Name", "Sex", "Fare")

# 5. Escribir los resultados en la consola en tiempo real
query = filtered_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
