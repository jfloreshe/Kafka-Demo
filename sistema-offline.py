from kafka import KafkaProducer
from faker import Faker
import json
from datetime import datetime
import time
fake = Faker()
Faker.seed(0)
class Donation:
    def __init__(self):
        self.user = fake.name()
        self.address = fake.address()
        self.card = fake.credit_card_full()
        self.finalDonation = fake.pricetag()
        self.date = fake.date()

print("Connection detected ... Sending data to Kafka")
producer =  KafkaProducer(bootstrap_servers='localhost:9092')

for _ in range(100):
    obj = Donation()
    jsonStr = json.dumps(obj.__dict__)
    kafkaMessage = jsonStr.encode()
    print('-----------------------------------------------------------------')
    print("Message sent at "+ datetime.now().strftime("%d/%m/%Y %H:%M:%S")+'\n' + "[Topic: donations] message: \n"+jsonStr)
    time.sleep(0.5)
    producer.send('donations', kafkaMessage)

