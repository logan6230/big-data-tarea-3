from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when, round, avg
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType

# Configurar SparkSession
spark = SparkSession.builder \
    .appName("GreenhouseDataAnalysisWithAlerts") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
    StructField("greenhouse_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("sensors", StructType([
        StructField("sensor_1", StructType([
            StructField("temperature", FloatType(), True),
            StructField("humidity", FloatType(), True)
        ]), True),
        StructField("sensor_2", StructType([
            StructField("temperature", FloatType(), True),
            StructField("humidity", FloatType(), True)
        ]), True)
    ]), True)
])

# Leer el flujo de datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "greenhouse_data") \
    .load()

# Parsear los datos del valor en formato JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Aplanar los datos de sensores
flattened_df = parsed_df.select(
    col("greenhouse_id"),
    col("timestamp"),
    col("sensors.sensor_1.temperature").alias("sensor_1_temp"),
    col("sensors.sensor_1.humidity").alias("sensor_1_hum"),
    col("sensors.sensor_2.temperature").alias("sensor_2_temp"),
    col("sensors.sensor_2.humidity").alias("sensor_2_hum")
)

# Añadir watermark para manejar datos tardíos (por ejemplo, 5 minutos de tolerancia)
windowed_stats = flattened_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), "greenhouse_id") \
    .agg(
        round(avg("sensor_1_temp"), 3).alias("avg_sensor_1_temp"),
        round(avg("sensor_1_hum"), 3).alias("avg_sensor_1_hum"),
        round(avg("sensor_2_temp"), 3).alias("avg_sensor_2_temp"),
        round(avg("sensor_2_hum"), 3).alias("avg_sensor_2_hum")
    ) \
    .withColumn(
        "alert_sensor_1_temp", 
        when((col("avg_sensor_1_temp") > 28) | (col("avg_sensor_1_temp") < 17), "Temperature Alert").otherwise("")
    ) \
    .withColumn(
        "alert_sensor_1_hum", 
        when((col("avg_sensor_1_hum") > 78) | (col("avg_sensor_1_hum") < 30), "Humidity Alert").otherwise("")
    ) \
    .withColumn(
        "alert_sensor_2_temp", 
        when((col("avg_sensor_2_temp") > 28) | (col("avg_sensor_2_temp") < 17), "Temperature Alert").otherwise("")
    ) \
    .withColumn(
        "alert_sensor_2_hum", 
        when((col("avg_sensor_2_hum") > 78) | (col("avg_sensor_2_hum") < 30), "Humidity Alert").otherwise("")
    )

# Escribir los resultados en la consola con outputMode "complete"
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Iniciar el stream y esperar su finalización
query.awaitTermination()
