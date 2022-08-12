from ensurepip import bootstrap
from kafka import KafkaConsumer
from kafka import TopicPartition
from datetime import datetime
import json

if __name__ == "__main__":
    consumer = KafkaConsumer("events")
    #consumer.assign([TopicPartition('demo',0)])
    print("starting consumer")
    for msg in consumer:
        print("Message received at "+ datetime.now().strftime("%d/%m/%Y %H:%M:%S")+msg.value.decode())