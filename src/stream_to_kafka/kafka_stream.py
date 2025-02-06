import requests
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
def create_kafka_topic(topic_name,num_partitions=1,replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers="kafka:9092")
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name,num_partitions=num_partitions,replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[topic],validate_only=False)
        admin_client.close()
def get_data():
    url = 'https://my.api.mockaroo.com/transaction_pipeline.json?key=7055dd70'
    r = requests.get(url)
    data = r.json()
    # file_path = "/app/pipeline_project.json"
    # with open(file_path,'r') as file:
    #     data = json.load(file)
    print(json.dumps(data, indent=4))
    producer = KafkaProducer(bootstrap_servers='kafka:9092',key_serializer=lambda k: k.encode('utf-8'),value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for row in data:
        key = str(row['transaction_id'])
        del row['transaction_id']
        value = row
        producer.send('test1',key=key,value=value)
    producer.flush()
    producer.close()
def main():
    create_kafka_topic("test1") 
    get_data()
main()