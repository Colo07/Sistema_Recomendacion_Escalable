# Definir una imagen base
FROM wurstmeister/zookeeper:latest

# Exponer el puerto de Zookeeper
EXPOSE 2181

# Crear la imagen de Zookeeper
#docker build -t zookeeper-server -f zookeeper.dockerfile ..
# Ejecutar el contenedor de Zookeeper
#docker run -d --name zookeeper zookeeper-server