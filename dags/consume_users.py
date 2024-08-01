from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,PROCCESED_USERS

# Crear una sesión de Spark
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

# Definir el esquema para los datos de usuarios
userSchema = StructType([
    StructField("user_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType())
])

# Leer datos de Kafka
usersDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "usuarios") \
    .option("startingOffsets", "earliest") \
    .option("kafka.max.poll.records", "500") \
    .load()


# Seleccionar y deserializar el valor de Kafka
usersDF = usersDF.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), userSchema).alias("data")) \
    .select("data.*")


usersJsonDF = usersDF.select(to_json(struct(
    col("user_id"), col("name"), col("age"), col("gender")
)).alias("value"))


# Mostrar los datos en la consola (para pruebas)
user_query = usersDF.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", PROCCESED_USERS) \
    .option("checkpointLocation", PROCCESED_USERS+"checkpoints") \
    .start()

user_query.awaitTermination()


