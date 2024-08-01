from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_user():
    user = {
        'user_id': random.randint(1, 100),
        'name': 'User' + str(random.randint(1, 100)),
        'age': random.randint(18, 70),
        'gender': random.choice(['M', 'F'])
    }
    producer.send('usuarios', user)
    producer.flush()

while True:
    produce_user()
    time.sleep(3)  # Espera de 5 segundos entre cada usuario
