import json
import time
import requests
import os
from kafka import KafkaProducer
from dotenv import load_dotenv


# API Settings
load_dotenv()
API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITY = 'Flushing'
COUNTRY = 'USA'
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY},{COUNTRY}&appid={API_KEY}'

#Kafka settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROOKER = 'localhost:9092'

def get_weather_data():
    response = requests.get(URL)
    data = response.json()
    return data

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROOKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        weather_data = get_weather_data()
        producer.send(KAFKA_TOPIC, weather_data)
        print(f'Sent data: {weather_data}')
        time.sleep(60)

if __name__ == "__main__":
    main()