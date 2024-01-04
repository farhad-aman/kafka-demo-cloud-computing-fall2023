from kafka import KafkaConsumer
import json
import threading


def consume():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='test-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print(f"Thread {threading.current_thread().name} received: {message.value} from partition: {message.partition}")


threads = []
for i in range(2):
    thread = threading.Thread(target=consume, name=f"Consumer-{i}")
    threads.append(thread)
    thread.start()
