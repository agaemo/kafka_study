"""
マーケティング班としてnewspaper-topicを受信する。
引数で識別名を指定: python phase2/consumer.py A
"""
import sys
from kafka import KafkaConsumer
import json

TOPIC = "newspaper-topic"
GROUP_ID = "marketing-team"

name = sys.argv[1] if len(sys.argv) > 1 else "Unknown"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[メンバー {name}] 受信待機中（Group: {GROUP_ID}）")

try:
    for message in consumer:
        print(
            f"[メンバー {name}] partition={message.partition} offset={message.offset} title={message.value['title']}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
