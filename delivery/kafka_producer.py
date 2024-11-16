from confluent_kafka import Producer
import json
import os
import time


conf = {
    'bootstrap.servers': '192.168.1.6:9092',
    'auto.offset.reset': 'smallest',
}


producer = Producer(conf)

starting_latitude, ending_latitude = 19.0760, 18.5204
starting_longitude, ending_longitude = 72.8777, 73.8567


num_steps = 1000
step_size_latitude = (ending_latitude - starting_latitude) / num_steps
step_size_longitude = (ending_longitude - starting_longitude) / num_steps
current_steps = 0


def delivery_report(err, msg):
    if err is not None:
        print(f"Msg delivery failed: {err}")
    else:
        print(f"Msg delivered Successfully to {msg.topic()} [{msg.partition()}]")



topic = "location_updates"
while True:
    latitude = starting_latitude + step_size_latitude * current_steps
    longitude = starting_longitude + step_size_longitude * current_steps


    data = {
        "latitude": latitude,
        "longitude": longitude
    }

    print(data)

    producer.produce(topic, json.dumps(data).encode("utf-8"), callback = delivery_report)
    producer.flush()

    current_steps+=1

    if current_steps > num_steps:
        current_steps = 0

    time.sleep(2)


