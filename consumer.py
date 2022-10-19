from time import sleep
from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'testTopic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

while True:
    for message in consumer:
        print ("Message", message)
        if message is not None:
            print (message.offset, message.value)
