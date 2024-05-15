import json
import folium
from kafka import KafkaConsumer

# Kafka Settings
KAFKA_TOPIC = 'weather_data'
KAFKA_BROKER = 'localhost:9092'

# Weather Map
def generate_map(weather_data):
    lat = weather_data['coord']['lat']
    lon = weather_data['coord']['lon']
    description = weather_data['weather'][0]['description']
    temp = weather_data['main']['temp'] - 459.67 # Convert from kelvin to fer
    city = weather_data['name']

    map_ = folium.Map(location=[lat, lon], zoom_start=10)
    popup_text = f"City: {city}<br>Temperature: {temp:.2f}°F<br>Description: {description}"
    folium.Marker([lat, lon], popup=popup_text).add_to(map_)
    map_.save('templates/weather_map.html')

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
        generate_map(weather_data)

if __name__ == "__main__":
    main()