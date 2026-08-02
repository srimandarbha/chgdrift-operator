import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('simulate_chg.json') as f:
    payload = json.load(f)

producer.send('gitops.chg.events', payload)
producer.flush()
print("Simulated CHG event successfully published to gitops.chg.events!")