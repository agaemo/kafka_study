"""キーを指定してPartitionを固定する。同じキーは同じPartitionに入る。"""
from kafka import KafkaProducer
import json
import time

TOPIC = "offset-topic"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

users = ["user-001", "user-002", "user-003"]

for i in range(9):
    user_id = users[i % 3]
    message = {"user_id": user_id, "action": f"イベント {i}"}
    producer.send(TOPIC, key=user_id, value=message)
    print(f"送信: key={user_id} value={message}")
    time.sleep(0.3)

producer.flush()
producer.close()
print("完了")
