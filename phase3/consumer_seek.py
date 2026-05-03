"""
各Partitionのオフセットを0にリセットして最初から再処理する。
障害後の再処理ユースケースを模倣する。
"""
from kafka import KafkaConsumer, TopicPartition
import json

TOPIC = "offset-topic"
GROUP_ID = "offset-group"
PARTITION_COUNT = 3

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    group_id=GROUP_ID,
    enable_auto_commit=False,  # 手動でオフセットを管理する
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

partitions = [TopicPartition(TOPIC, i) for i in range(PARTITION_COUNT)]
consumer.assign(partitions)

# 全Partitionをオフセット0に移動する
for partition in partitions:
    consumer.seek(partition, 0)

print(f"オフセット0から再処理開始（Group: {GROUP_ID}）")

try:
    # 各Partitionの末尾まで読んで終了する
    end_offsets = consumer.end_offsets(partitions)
    processed = 0
    total = sum(end_offsets.values())

    while processed < total:
        records = consumer.poll(timeout_ms=1000)
        for _, messages in records.items():
            for message in messages:
                print(
                    f"再処理: partition={message.partition} offset={message.offset} value={message.value}"
                )
                processed += 1
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

print("再処理完了")
