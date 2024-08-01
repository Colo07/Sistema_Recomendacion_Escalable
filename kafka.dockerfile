# Definir imagen base
FROM wurstmeister/kafka:latest
EXPOSE 9092 9093

# Configurar las variables de entorno para el servidor de Zookeeper
ENV KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
ENV KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
ENV KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
ENV KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT

# Crear la imagen de Kafka
#docker build -t kafka-server -f kafka.dockerfile .
# Ejecutar el contenedor de Kafka
#docker run -d --name kafka --link zookeeper:zookeeper kafka-server