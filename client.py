import json
import random
import threading
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer

API_URL = "http://127.0.0.1:5000"
EMOJIS = ["🔥", "😂", "😢", "😎", "😡", "🏏", "🤡"]
CLUSTERS = ["c1", "c2", "c3"]

CLIENTS = []

# MAKE A CLASS CLIENT FOR EACH CLIENT THAT REGISTERS


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
                print(f"[SENT] [{emoji_type}]")

            time.sleep(1)
            # time.sleep(0.05)

        except requests.exceptions.RequestException as e:
            print(f"\n[Client {client_id}] Error sending emoji: {e}")
            time.sleep(1)


if __name__ == "__main__":
    threading.Thread(target=send_emoji, args=(1,)).start()
