"""newspaper-topic にニュース記事を15件送信する。"""
from kafka import KafkaProducer
import json
import time

TOPIC = "newspaper-topic"

articles = [
    "政府が新たな経済対策を発表",
    "株式市場が急騰、過去最高値を更新",
    "プロ野球開幕戦で接戦",
    "首都圏で大規模システム障害",
    "国際会議で気候変動対策を議論",
    "新薬の臨床試験で有望な結果",
    "サッカー代表チームが強化合宿",
    "半導体不足が製造業に影響",
    "大学入試改革の方針を文科省が公表",
    "春の高校野球が開幕",
    "円相場が1ドル150円台に",
    "AIを活用した新サービスが続々登場",
    "国内旅行者数がコロナ前の水準に回復",
    "新興国市場への投資が拡大",
    "夏の甲子園出場校が決定",
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i, title in enumerate(articles):
    message = {"id": i, "title": title}
    producer.send(TOPIC, message)
    print(f"送信: {message}")
    time.sleep(0.5)

producer.flush()
producer.close()
print("完了")
