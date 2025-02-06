import sys
import os
sys.path.insert(0,os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
import requests
import json
from kafka import KafkaProducer
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from src.stream_to_kafka.kafka_stream import main
from airflow.providers.docker.operators.docker import DockerOperator

# def get_data():
#     url = 'https://my.api.mockaroo.com/pipeline_project.json?key=66e54260'
#     r = requests.get(url)
#     data = r.json()
#     print(json.dumps(data, indent=4))
#     producer = KafkaProducer(bootstrap_servers='kafka:9092',key_serializer=lambda k: k.encode('utf-8'),value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#     for row in data:
#         key = str(row['transaction_id'])
#         del row['transaction_id']
#         value = row
#         producer.send('test1',key=key,value=value)
#     producer.flush()
#     producer.close()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=80),
}

with DAG(
    dag_id = "run_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date = datetime.today()
) as dag:
    stream_to_data = PythonOperator(
        task_id = 'stream_to_data',
        python_callable = main
    )
    stream_from_data = DockerOperator(
        task_id = 'stream_from_data',
        image = "write_to_database:latest",
        api_version='auto',
        auto_remove=True,
        command="./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:2.8.0,mysql:mysql-connector-java:8.0.30,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 ./stream_from_kafka.py",
        docker_url="tcp://docker-proxy:2375",
        depends_on_past=False,
        trigger_rule="all_success",
        environment={
        'MYSQL_HOST': 'mysql',
        'MYSQL_PORT': '3306',
        'MYSQL_DB': 'project',
        'MYSQL_USER': 'root',
        'MYSQL_PASSWORD':'1',
        'CASSANDRA_HOST': 'cassandra',
        'CASSANDRA_PORT': '9042',
        'CASSANDRA_USER': 'cassandra',
        'CASSANDRA_PASSWORD': 'cassandra',
        'CASSANRA_KEYSPACE': 'my_keyspace',
        'CASSANDRA_LOCAL_DC': 'datacenter1',},
        network_mode='nextproject_airflow-kafka',
        mount_tmp_dir=False,
        dag=dag
    )
    stream_to_data >> stream_from_data