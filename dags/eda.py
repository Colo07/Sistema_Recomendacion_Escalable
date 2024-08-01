from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import collect_list, col, from_json,expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
import boto3,os
from config import AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,PROCCESED_USERS,PROCCESED_TRANSACTIONS,PROCCESED_PRODUCTS,EMBEDDINGS_PATH, S3_BUCKET
# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("EDA") \
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
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True)
    #StructField("amount", FloatType(), True),
])

# Definir el esquema para los datos de usuarios
userSchema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True)
])

# Definir el esquema para los datos de productos
productSchema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True)
])

# Leer datos de S3
transactionsDF = spark.read.schema(transactionSchema).json(PROCCESED_TRANSACTIONS)
usersDF = spark.read.schema(userSchema).json(PROCCESED_USERS)
productsDF = spark.read.schema(productSchema).json(PROCCESED_PRODUCTS)

# Eliminar duplicados y manejar valores nulos
cleanedTransactionsDF = transactionsDF.dropDuplicates(["transaction_id", "product_id", "user_id","timestamp"]).na.drop()
cleanedProductsDF = productsDF.dropDuplicates(["product_id", "name", "category", "price"]).na.drop()
cleanedUsersDF = usersDF.dropDuplicates(["user_id", "name", "age", "gender"]).na.drop()

# Convertir product_id a string
cleanedTransactionsDF = cleanedTransactionsDF.withColumn("product_id", col("product_id").cast("string"))

# Preparar datos para Word2Vec
userProductSeqDF = cleanedTransactionsDF.groupBy("user_id").agg(collect_list("product_id").alias("buyed_products"))

# Convertir los valores en la columna product_sequence a cadenas de texto
userProductSeqDF = userProductSeqDF.withColumn("buyed_products", expr("transform(buyed_products, x -> cast(x as string))"))

# Entrenar el modelo Word2Vec
word2Vec = Word2Vec(vectorSize=5, minCount=3, inputCol="buyed_products", outputCol="product_embeddings")
model = word2Vec.fit(userProductSeqDF)

# Transformar los datos para obtener embeddings
embeddingsDF = model.transform(userProductSeqDF)

# Guardar embeddings en AWS S3
embeddingsDF.write \
    .format("json") \
    .mode("overwrite") \
    .save(EMBEDDINGS_PATH)

#Guardar local
model_path = ("C:/Users/Lenovo/Desktop/Sistema_De_Recomendacion_Escalable/Sistema_de_Recomendacion_Escalable/word2vec_model")
model.write().overwrite().save(model_path)

# Configurar las credenciales de AWS
aws_access_key_id = AWS_ACCESS_KEY_ID
aws_secret_access_key = AWS_SECRET_ACCESS_KEY
aws_region = "us-east-2"
bucket_name = S3_BUCKET
s3_model_path = "models/word2vec_model/"

# Subir modelo a S3
s3_client = boto3.client('s3', region_name=aws_region, 
                         aws_access_key_id=aws_access_key_id, 
                         aws_secret_access_key=aws_secret_access_key)


for root, dirs, files in os.walk(model_path):
    for file in files:
        local_file_path = os.path.join(root, file)
        s3_file_path = os.path.relpath(local_file_path, model_path)
        s3_client.upload_file(local_file_path, bucket_name, os.path.join(s3_model_path, s3_file_path))

#Windows guarda diferente a S3 y Linux. En s3 tuve que crear 2 carpetas data y metadata, y borrar 
#de cada archivo que se subio, el prefijo correspondiente para poder hacer un load correcto en app.py