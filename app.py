from flask import Flask, request, jsonify
from pyspark.sql import Row
import boto3
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2VecModel
import os
from pyspark.ml.linalg import Vectors,DenseVector
import json
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,S3_BUCKET,MODEL_PATH,EMBEDDINGS_PATH

spark = SparkSession.builder \
    .appName("RecommenderAPI") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
    .getOrCreate()


s3_bucket = S3_BUCKET
model_path = MODEL_PATH
embeddings_path = EMBEDDINGS_PATH

# Descargar y cargar el modelo Word2Vec desde S3
try:
    word2VecModel = Word2VecModel.load(model_path)
except Exception as e:
    print(f"Error al cargar el modelo: {e}")


def get_user_embeddings(user_id):
    # Obtener los embeddings de productos para el usuario desde S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    response = s3.get_object(Bucket=S3_BUCKET, Key=f"embeddings/part-00000-9284e67c-fc08-4dfb-8373-9d6dceb8ed57-c000.json")
    part_file_content = response['Body'].read().decode('utf-8')
    lines = part_file_content.strip().split('\n')

    # Buscar el embedding del usuario
    for line in lines:
        data = json.loads(line)
        if data['user_id'] == user_id:
            return DenseVector(data['product_embeddings']['values'])
    raise ValueError(f"No se encontraron embeddings para el usuario con ID {user_id}")  


app = Flask(__name__)
@app.route('/recommend', methods=['POST'])
def recommend():
    try:
        data = request.get_json()
        user_id = data['user_id']
        
        # Obtener los embeddings de productos del usuario
        user_embeddings = get_user_embeddings(user_id)
        # Generar recomendaciones
        recommendations = word2VecModel.findSynonyms(user_embeddings, 10)
        recommendations_list = recommendations.collect()
        
        #Para una mejor lectura 
        recommendations_readable = [
            {"product_id": rec["word"], "similarity": rec["similarity"]}
            for rec in recommendations_list
        ]
        return jsonify(recommendations_readable)
    except Exception as e:
        return str(e), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)