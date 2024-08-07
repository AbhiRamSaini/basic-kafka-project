from confluent_kafka import Consumer, KafkaError
import json

# Create Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['weather-data'])

# Dictionary to store average temperatures
city_temps = {}

# Main loop
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data = json.loads(msg.value().decode('utf-8'))
    city = data['city']
    temp = data['temperature']
    
    if city not in city_temps:
        city_temps[city] = {'total': temp, 'count': 1}
    else:
        city_temps[city]['total'] += temp
        city_temps[city]['count'] += 1
    
    avg_temp = city_temps[city]['total'] / city_temps[city]['count']
    
    print(f"Received: {data}")
    print(f"Average temperature in {city}: {avg_temp:.1f}°C")
    print("---")

consumer.close()