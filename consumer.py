import json
from kafka import KafkaConsumer

# Kafka Settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROKER = 'localhost:9092'

# Kafka Consumer
consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='weather-group',
                         value_serializer=lambda v: json.dump(v).encode('utf-8'))

for message in consumer:
    weather_data = json.loads(message.value)
    print(f'Recived weather data: {weather_data}')