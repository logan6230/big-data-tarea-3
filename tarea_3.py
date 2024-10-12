# Importamos las librerías necesarias para el análisis con PySpark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import regexp_replace, split, explode, col, to_date, hour

# Inicializa una sesión de Spark para procesar los datos
spark = SparkSession.builder.appName('SimplifiedSentimentAnalysis').getOrCreate()

# Definimos la ruta del archivo .csv (conjunto de datos de sentimientos)
file_path = 'hdfs://localhost:9000/Tarea_3_2/sentimentdataset.csv'

# Cargamos el archivo .csv en un DataFrame de Spark
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# 1. Distribución de Sentimientos por Plataforma y País
sentiment_by_platform_country = df.groupBy('Platform', 'Country', 'Sentiment').count().orderBy('Platform', 'Country', 'count', ascending=False)
print("Distribución de Sentimientos por Plataforma y País:")
sentiment_by_platform_country.show()

# 2. Análisis Temporal de Sentimientos
# Añadimos una columna con la hora del día extraída de la fecha/hora (Timestamp)
df_with_time = df.withColumn('Hour', hour('Timestamp'))

# Agrupamos por hora del día y tipo de sentimiento
sentiment_by_hour = df_with_time.groupBy('Hour', 'Sentiment').count().orderBy('Hour')
print("Distribución de Sentimientos por Hora del Día:")
sentiment_by_hour.show()

# 3. Impacto de los Sentimientos en la Participación
# Calculamos el promedio de retweets y likes para cada tipo de sentimiento
sentiment_engagement = df.groupBy('Sentiment').agg(F.avg('Retweets').alias('Avg_Retweets'), F.avg('Likes').alias('Avg_Likes')).orderBy('Sentiment')
print("Promedio de Retweets y Likes por Sentimiento:")
sentiment_engagement.show()

# 4. Usuarios más Activos y su Sentimiento Predominante
# Agrupamos por usuario y sentimiento para saber qué usuarios publican más tweets y qué sentimientos expresan
user_sentiment = df.groupBy('User', 'Sentiment').agg(F.count('*').alias('Tweet_Count')).orderBy(F.col('Tweet_Count').desc())
print("Usuarios más activos y su Sentimiento Predominante:")
user_sentiment.show(10)

# 5. Análisis de Hashtags
# Extraemos los hashtags y contamos cuántas veces aparece cada uno
hashtags_counts = df.select(explode(split(col('Hashtags'), ' ')).alias('Hashtag')).groupBy('Hashtag').count().orderBy(col('count').desc())
print("Top 10 Hashtags más usados:")
hashtags_counts.show(10)

# 6. Distribución de Tweets por Plataforma
tweets_by_platform = df.groupBy('Platform').count().orderBy(col('count').desc())
print("Conteo de Tweets por Plataforma:")
tweets_by_platform.show()

# Almacenar los resultados en formato JSON en un solo archivo en /home/sepantojad

# Define la ruta de salida
output_path = '/home/sepantojad/'

# Guardar cada resultado en un solo archivo JSON
# Usamos coalesce(1) para reducir a una partición y mode('overwrite') para evitar la creación de múltiples archivos
sentiment_by_platform_country.coalesce(1).write.mode('overwrite').json(output_path + 'sentiment_by_platform_country.json')
sentiment_by_hour.coalesce(1).write.mode('overwrite').json(output_path + 'sentiment_by_hour.json')
sentiment_engagement.coalesce(1).write.mode('overwrite').json(output_path + 'sentiment_engagement.json')
user_sentiment.coalesce(1).write.mode('overwrite').json(output_path + 'user_sentiment.json')
hashtags_counts.coalesce(1).write.mode('overwrite').json(output_path + 'hashtags_counts.json')
tweets_by_platform.coalesce(1).write.mode('overwrite').json(output_path + 'tweets_by_platform.json')

# Cierre de la sesión de Spark
spark.stop()
