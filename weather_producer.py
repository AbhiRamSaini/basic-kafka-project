from confluent_kafka import Producer
import json
import time
import random

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'weather_producer'
}

# Create Kafka producer
producer = Producer(conf)

# List of cities
cities = ['New York', 'London', 'Paris', 'Tokyo', 'Sydney']

# Function to generate weather data
def generate_weather():
    return {
        'city': random.choice(cities),
        'temperature': round(random.uniform(-5, 35), 1),
        'humidity': random.randint(30, 90),
        'timestamp': int(time.time())
    }

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Main loop
try:
    while True:
        weather_data = generate_weather()
        
        # Produce message
        producer.produce(
            'weather-data',
            key=weather_data['city'],
            value=json.dumps(weather_data),
            callback=delivery_report
        )
        
        print(f"Sent: {weather_data}")
        
        # Trigger any available delivery report callbacks
        producer.poll(0)
        
        time.sleep(1)  # Wait for 1 second

except KeyboardInterrupt:
    print("Interrupted by user, shutting down...")
finally:
    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered
    producer.flush()