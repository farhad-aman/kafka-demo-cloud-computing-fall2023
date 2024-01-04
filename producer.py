from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    key = f"key-{i % 12}"
    message = {'number': i}
    producer.send('test-topic', key=key, value=message)
    print(f"Sent: {message} with key: {key}")
    time.sleep(1)

producer.close()
