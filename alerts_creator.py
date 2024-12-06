from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config, MY_NAME
import json
import uuid

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

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name_1 = f'{MY_NAME}_building_sensors'
topic_name_2 = f'{MY_NAME}_temperature_alerts'
topic_name_3 = f'{MY_NAME}_humidity_alerts'

consumer.subscribe([topic_name_1])

print(f"Subscribed to topic '{topic_name_1}'")

try:
    for message in consumer:
        if message.value['temperature_value'] > 40:
            producer.send(topic_name_2, key=str(uuid.uuid4()), value=message.value)
            producer.flush()
            print(f"Temperature alert: {message.value} was sent to topic '{topic_name_2}'")
        if message.value['humidity_value'] < 20 or message.value['humidity_value'] > 80:
            producer.send(topic_name_3, key=str(uuid.uuid4()), value=message.value)
            producer.flush()
            print(f"Humidity alert: {message.value} was sent to topic '{topic_name_3}'")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Close consumer