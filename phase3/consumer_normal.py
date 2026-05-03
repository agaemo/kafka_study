"""通常の受信。オフセットはKafkaが自動管理する。Ctrl+C で停止。"""
from kafka import KafkaConsumer
import json

TOPIC = "offset-topic"
GROUP_ID = "offset-group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"受信待機中（Group: {GROUP_ID}）")

try:
    for message in consumer:
        print(
            f"partition={message.partition} offset={message.offset} key={message.key} value={message.value}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
