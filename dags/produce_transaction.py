from kafka import KafkaProducer
import json
import time
import random

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Función para generar un ID de transacción único
transaction_id = 0

def produce_transaction():
    global transaction_id
    transaction = {
        'transaction_id': transaction_id,
        'product_id': random.randint(1, 100),
        'user_id': random.randint(1, 100),
        'quantity': random.randint(1, 10),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    }
    producer.send('transacciones', transaction)
    producer.flush()
    transaction_id += 1

# Enviar datos de transacciones continuamente
while True:
    produce_transaction()
    time.sleep(3)  # Espera de 1 segundo entre cada transacción
