import requests
import json
import folium
import os
from flask import Flask, render_template, request, redirect, url_for, jsonify

app = Flask(__name__)

KAFKA_PRODUCER_URL = 'http://localhost:5001/fetch_weather'

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        location = request.form['location']
        response = requests.post(KAFKA_PRODUCER_URL, json={'location': location})
        if response.status_code == 200:
            weather_data = response.json()['data']
            generate_map(weather_data)
            return redirect(url_for('weather_map'))
        else: 
            return render_template('index.html', error="Could not fetch weather data for the specified location.")
    return render_template('index.html')
    

@app.route('/map')
def weather_map():
    return render_template('weather_map.html')

def generate_map(weather_data):
    lat = weather_data['coord']['lat']
    lon = weather_data['coord']['lon']
    description = weather_data['weather'][0]['description']
    temp_c = weather_data['main']['temp'] - 273.12 # Convert from kelvin to fer
    temp_f = (temp_c * 9/5) + 32
    city = weather_data['name']

    map_ = folium.Map(location=[lat, lon], zoom_start=10)
    popup_text = f"City: {city}<br>Temperature: {temp_f:.2f}°F<br>Description: {description}"
    folium.Marker([lat, lon], popup=popup_text).add_to(map_)
    map_.save('templates/weather_map.html')

    with open('static/weather_map.html', 'a') as f:
        f.write('''
            <div style="position: absolute; bottom: 10px; left: 10px;">
            <a href="/" style="background-color: white; padding: 10px; border: 1px solid black; text-decoration: none;">Go Back</a>
        </div>
        ''')

if __name__ == "__main__":
    if not os.path.exists('static'):
        os.makedirs('static')
    app.run(debug=True)