import json
import random
import threading
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer

API_URL = "http://127.0.0.1:5000"
EMOJIS = ["🔥", "😂", "😢", "😎", "😡", "🏏", "🤡", "🐐"]
# CLUSTERS = ["c1", "c2", "c3"]
CLUSTERS = ["c1"]

SUBSCRIBERS = ["s1", "s2", "s3"]
subs = random.choice(SUBSCRIBERS)
clus = random.choice(CLUSTERS)

TO_JOIN = clus + subs


def register():
    pass


def send_emoji(client_id):
    while True:

        try:

            emoji_type = random.choice(EMOJIS)
            timestamp = int(time.time())

            data = {
                "user_id": f"user_{client_id}",
                "emoji_type": emoji_type,
                "timestamp": timestamp,
            }

            response = requests.post(f"{API_URL}/emoji", json=data)

            if response.status_code == 200:
                pass
                # print(f"[SENT] [{emoji_type}]")

            # time.sleep(1)
            time.sleep(0.05)

        except requests.exceptions.RequestException as e:
            print(f"\n[Client {client_id}] Error sending emoji: {e}")
            time.sleep(1)


def receive_final_data():

    consumer = KafkaConsumer(
        TO_JOIN,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
        data = message.value
        print(f"[RECEIVED] [{TO_JOIN}] {data['max_emoji']}")


if __name__ == "__main__":
    threading.Thread(target=send_emoji, args=(1,)).start()
    threading.Thread(target=receive_final_data).start()
