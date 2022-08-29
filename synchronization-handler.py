from kafka import KafkaConsumer
from datetime import datetime
import json
import time

if __name__ == "__main__":
    consumer = KafkaConsumer("donations")    
    print("starting consumer of donations")
    for i, msg in enumerate(consumer):
        print('-----------------------------------------------------------------')
        print("Message received at "+ datetime.now().strftime("%d/%m/%Y %H:%M:%S")+'\n'+'Message:\n'+msg.value.decode(), '\n')
        print("Saving the register into centralized database ....")
        print('Donation saved succesfully with id: '+str(i))
        time.sleep(1)
