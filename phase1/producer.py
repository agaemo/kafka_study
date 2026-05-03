"""hello-topic にメッセージを5件送信する。"""
from kafka import KafkaProducer
import json
import time

TOPIC = "hello-topic"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(5):
    message = {"id": i, "text": f"メッセージ {i}"}
    producer.send(TOPIC, message)
    print(f"送信: {message}")
    time.sleep(1)

producer.flush()
producer.close()
print("完了")
