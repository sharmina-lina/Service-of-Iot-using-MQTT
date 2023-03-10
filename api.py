# import Libraries
from pymongo import MongoClient
from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
import math

# Connect to MQTT server
mqtt_client = mqtt.Client()

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bus_data']

# Set up Flask app
app = Flask(__name__)

# Create Subscribtion
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT server with result code "+str(rc))
    mqtt_client.subscribe("/hfp/v2/journey/#")

def on_message(client, userdata, msg):
    try:
        message = json.loads(msg.payload)
        collection = db['telemetry']
        collection.insert_one(message)  # store data to the databse(Collection)
    except Exception as e:
        print("Error: ", e)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("mqtt.hsl.fi", 1883, 60)

mqtt_client.loop_start()

# Define API endpoints
@app.route('/buses', methods=['GET'])
def get_buses():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    radius = request.args.get('radius', default=1000, type=int)
    
    if lat is None or lon is None:
        return jsonify({'error': 'Please provide latitude and longitude values.'})
    
    lat_max = float(lat) + (radius/111000)
    lat_min = float(lat) - (radius/111000)
    lon_max = float(lon) + (radius/111000)/abs(math.cos(float(lat))*111000)
    lon_min = float(lon) - (radius/111000)/abs(math.cos(float(lat))*111000)
    
    collection = db['telemetry']            # make the telemetry searchable
    buses = collection.find({
        '$and': [
            {'VP.lat': {'$gte': lat_min, '$lte': lat_max}},
            {'VP.long': {'$gte': lon_min, '$lte': lon_max}}
        ]
    }).sort([
        ('distance', 1)
    ]).limit(50)
    
    response = []
    for bus in buses:
        bus_lat = bus['VP']['lat']
        bus_lon = bus['VP']['long']
        distance = math.sqrt((bus_lat - float(lat))**2 + (bus_lon - float(lon))**2)
        next_stop = bus.get('next_stop', {}).get('name', '')
        
        response.append({
            'id': str(bus['_id']),
            'lat': bus['VP']['lat'],
            'lon': bus['VP']['long'],
            'Bus Line No': bus['VP']['line'],
            'next_stop': next_stop,
            'distance' : distance
        })
        
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
