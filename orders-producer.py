
# kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic orders
# kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning


# cd workshop/kafka
# python orders-producer.py

import random
from confluent_kafka import Producer
import time
import json 

DELAY = 2
SAMPLES =  100000
TOPIC = "orders"

statelist = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
           'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
           'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
           'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
           'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# at very first producer shalll upload schema to schema registry

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(SAMPLES):
   order_id = int(random.randint(1, 100000))
   state = random.choice(statelist)
   List_Items = int(random.randint(1, 5))
   for j in range(List_Items):
        item_id = str(random.randint(1, 100))
        price = int(random.randint(1, 50))
        Qty = int(random.randint(1, 10))
        value = { 
            "order_id": order_id,
            "item_id": item_id,
            "price": price,
            "Qty": Qty,
            "state" : state
        }

        key = "state"

        valueStr = json.dumps(value).encode('utf-8')

        print('valueStr ', valueStr)

        producer.produce(topic=TOPIC, value=valueStr, key=key)
        producer.flush()
        time.sleep(DELAY)