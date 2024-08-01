#Para correr la api a traves de docker correr
# Usar una imagen base oficial de Python
FROM python:3.12-slim

WORKDIR /Sistema_de_Recomendacion_Escalable

#Dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt requirements.txt
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]

# Instrucciones adicionales para construir y ejecutar la imagen Docker
# docker build -t recommender-api . --> Construye la imagen Docker
# docker run -p 5000:5000 recommender-api --> Corre localmente en el puerto 5000