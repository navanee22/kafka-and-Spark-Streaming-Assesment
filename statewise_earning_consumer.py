from confluent_kafka import Consumer
import json

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup21',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['statewise_earning'])


while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()


        # consumer = KafkaConsumer(bootstrap_servers='victoria.com:6667',
        #                          auto_offset_reset='earliest',
        #                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        # consumer.subscribe(['my-topic'])