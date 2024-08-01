from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,PROCCESED_TRANSACTIONS
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

# Definir el esquema para los datos de transacciones
transactionSchema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType())
])

# Leer datos de Kafka
transactionsDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones") \
    .option("startingOffsets", "earliest") \
    .option("kafka.max.poll.records", "500") \
    .load()

# Seleccionar y deserializar el valor de Kafka
transactionsDF = transactionsDF.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), transactionSchema).alias("data")) \
    .select("data.*")


transactionsJsonDF = transactionsDF.select(to_json(struct(
    col("transaction_id"), col("product_id"), col("user_id"), col("quantity"), col("timestamp")
)).alias("value"))


# Mostrar los datos en la consola (para pruebas)
transaction_query = transactionsDF.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", PROCCESED_TRANSACTIONS) \
    .option("checkpointLocation", PROCCESED_TRANSACTIONS+"checkpoints") \
    .start()

transaction_query.awaitTermination()

