"""hello-topic からメッセージを受信し続ける。Ctrl+C で停止。"""
from kafka import KafkaConsumer
import json

TOPIC = "hello-topic"
GROUP_ID = "hello-group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP_ID,
    auto_offset_reset="earliest",  # 起動時に最初から読む
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"受信待機中（Topic: {TOPIC}, Group: {GROUP_ID}）")

try:
    for message in consumer:
        print(
            f"受信: partition={message.partition} offset={message.offset} value={message.value}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
