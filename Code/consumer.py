import json
from kafka import KafkaConsumer

# Kafka Settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROKER = 'localhost:9092'

# Kafka Consumer
def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC, 
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    for message in consumer:
        weather_data = message.value
        print(f'Recived weather data: {weather_data}')

if __name__ == "__main__":
    main()