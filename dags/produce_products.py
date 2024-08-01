from kafka import KafkaProducer
import json
import time
import random

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lista de categorías de productos
categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Toys']

# Función para generar datos de productos aleatorios
def produce_product():
    product = {
        'product_id': random.randint(1, 100),
        'name': 'Product' + str(random.randint(1, 100)),
        'category': random.choice(categories),
        'price': round(random.uniform(5.0, 500.0), 2)
    }
    producer.send('productos', product)
    producer.flush()

# Enviar datos de productos continuamente
while True:
    produce_product()
    time.sleep(3)  # Espera de 3 segundos entre cada producto