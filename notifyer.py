from kafka import KafkaConsumer
from configs import kafka_config, MY_NAME
import json

# Create Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

topic_temperature = f'{MY_NAME}_temperature_alerts'
topic_humidity = f'{MY_NAME}_humidity_alerts'

consumer.subscribe([topic_temperature, topic_humidity])
print(f"Subscribed to topics '{topic_temperature, topic_humidity}'")

try:
    for message in consumer:
        print(f"Received message: {message.value} Message topic: {message.topic} with key: {message.key}, partition {message.partition}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Close consumer