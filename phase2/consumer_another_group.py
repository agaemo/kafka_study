"""ニュース切り抜き班として newspaper-topic を最初から読む。"""
from kafka import KafkaConsumer
import json

TOPIC = "newspaper-topic"
GROUP_ID = "news-clipping"  # marketing-team とは別のGroup

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[ニュース切り抜き班] 受信待機中（Group: {GROUP_ID}）")

try:
    for message in consumer:
        print(
            f"[切り抜き班] partition={message.partition} offset={message.offset} title={message.value['title']}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
