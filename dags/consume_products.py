from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,PROCCESED_PRODUCTS

# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Definir el esquema para los datos de productos
productSchema = StructType([
    StructField("product_id", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", FloatType())
])

# Leer datos de Kafka
productsDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "productos") \
    .option("startingOffsets", "earliest") \
    .option("kafka.max.poll.records", "500") \
    .load()

# Seleccionar y deserializar el valor de Kafka
productsDF = productsDF.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), productSchema).alias("data")) \
    .select("data.*")


# Convertir el DataFrame a una columna JSON string
productsJsonDF = productsDF.select(to_json(struct(
    col("product_id"), col("name"), col("category"), col("price")
    )).alias("value"))

# Escribir los datos en S3 en formato JSON
product_query = productsDF.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", PROCCESED_PRODUCTS) \
    .option("checkpointLocation", PROCCESED_PRODUCTS+"checkpoints") \
    .start()

# Esperar a que la consulta termine
product_query.awaitTermination()


#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 consume_products.py
