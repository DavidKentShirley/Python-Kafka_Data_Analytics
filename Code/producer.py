import json
import requests
import os
from kafka import KafkaProducer
from flask import Flask, request
from dotenv import load_dotenv

app = Flask(__name__)

# API Settings
load_dotenv()
API_KEY = os.getenv('OPENWEATHER_API_KEY')

#Kafka settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROOKER = 'localhost:9092'

producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROOKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_data(location):
    URL = f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={API_KEY}'
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    return None

@app.route('/fetch_weather', methods=['POST'])
def fetch_weather():
    location = request.json['location']
    weather_data = get_weather_data(location)
    if weather_data:
        producer.send(KAFKA_TOPIC, weather_data)
        return {'status': 'success', 'data': weather_data}, 200
    return {'status': 'error', 'message': 'Could not fetch weather data'}, 400

if __name__ == "__main__":
    app.run(port=5001)