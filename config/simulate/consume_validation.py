import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'gitops.change.validation',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=3000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Reading validation reports from gitops.change.validation...\n")
count = 0
for message in consumer:
    count += 1
    key = message.key.decode('utf-8') if message.key else 'None'
    print(f"--- [Message #{count} | Key: {key}] ---")
    print(json.dumps(message.value, indent=2))

if count == 0:
    print("No messages found in gitops.change.validation.")
