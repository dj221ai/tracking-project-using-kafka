import json
import os
import time
import sys
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer, KafkaException, KafkaError
from deliveryapp.models import LocationUpdate


class Command(BaseCommand):
    help = "Run Kafka consumer to listen for location update"

    def handle(self, *args, **options):
        conf = {
            'bootstrap.servers': '192.168.1.6:9092',
            'group.id': 'location_updates',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(**conf)
        consumer.subscribe(['location_updates'])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                data = json.loads(msg.value().decode('utf-8'))
                LocationUpdate.objects.create(
                    latitude=data['latitude'],
                    longitude=data['longitude']
                )

                print(f"Received msg and saved {data}")
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
        
        self.stdout.write("this is finally running !!!!")
