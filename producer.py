import requests
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv


# API Settings
load_dotenv()
API_KEY = os.getenv('30c4149190f0fc315a37b9e5db79dbbf')
CITY = 'Flushing'
API_URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'

#Kafka settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROOKER = 'localhost:9092'

# Create Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROOKER,
    value_serializer=lambda v: json.dump(v).encode('utf-8'))

while True:
    response = requests.get(API_URL)
    weather_data = response.json()

    #Send Data to Kafka
    producer.send(KAFKA_TOPIC, weather_data)
    print(f'Sent weather data: {weather_data}')

    # Sleep
    time.sleep(600)