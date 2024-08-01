from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_dag',
    default_args=default_args,
    description='Pipeline de ingesta continua con Airflow',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
)

start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='docker run -d --name zookeeper zookeeper-server',
    dag=dag,
)

start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command='docker run -d --name kafka --link zookeeper:zookeeper kafka-server',
    dag=dag,
)

start_producer_users_task = BashOperator(
    task_id='start_producer_users',
    bash_command='python Sistema_de_Recomendacion_Escalable/produce_users.py',
    dag=dag,
)

start_producer_transactions_task = BashOperator(
    task_id='start_producer_transactions',
    bash_command='python Sistema_de_Recomendacion_Escalable/produce_transaction.py',
    dag=dag,
)

start_producer_products_task = BashOperator(
    task_id='start_producer_products',
    bash_command='python Sistema_de_Recomendacion_Escalable/produce_products.py',
    dag=dag,
)

start_consumer_users_task = BashOperator(
    task_id='start_consumer_users',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 Sistema_de_Recomendacion_Escalable/consume_products.py',
    dag=dag,
)

start_consumer_transactions_task = BashOperator(
    task_id='start_consumer_transactions',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 Sistema_de_Recomendacion_Escalable/consume_transaction.py',
    dag=dag,
)

start_consumer_products_task = BashOperator(
    task_id='start_consumer_products',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 Sistema_de_Recomendacion_Escalable/consume_users.py',
    dag=dag,
)

eda_task = BashOperator(
    task_id='eda_task',
    bash_command='python Sistema_de_Recomendacion_Escalable/eda.py',
    dag=dag,
)

deploy_api_task = BashOperator(
    task_id='deploy_api',
    bash_command='docker run -d -p 5000:5000 recommender-api || docker start recommender-api',
    dag=dag,
)

# Definir las dependencias
start_zookeeper >> start_kafka

# Agregar dependencias para cada productor
start_kafka >> start_producer_products_task
start_kafka >> start_producer_users_task
start_kafka >> start_producer_transactions_task

# Agregar dependencias para cada consumidor
start_producer_products_task >> start_consumer_products_task
start_producer_users_task >> start_consumer_users_task
start_producer_transactions_task >> start_consumer_transactions_task

# EDA depende de la finalización de todos los consumidores
[start_consumer_products_task, start_consumer_users_task, start_consumer_transactions_task] >> eda_task

# Despliegue de la API depende de la finalización del EDA
eda_task >> deploy_api_task
