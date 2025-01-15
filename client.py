import json
import random
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request
from kafka import KafkaConsumer

API_URL = "http://127.0.0.1:5000"
EMOJIS = ["🔥", "😂", "😢", "😎", "😡", "🏏", "🤡", "🐐"]
# CLUSTERS = ["c1", "c2", "c3"]
CLUSTERS = ["c1"]

SUBSCRIBERS = ["s1", "s2", "s3"]
subs = random.choice(SUBSCRIBERS)
clus = random.choice(CLUSTERS)

TO_JOIN = clus + subs

NODE_ID = 1

heartbeat = {
    "node_id": NODE_ID,
    "message_type": "HEARTBEAT",
    "status": "UP",
    "timestamp": f"{datetime.now()}",
}


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
                log = {
                    "log_id": str(uuid.uuid4()),
                    "node_id": NODE_ID,
                    "log_level": "INFO",
                    "message_type": "LOG",
                    "message": f"SENT EMOJI TO SERVER [{emoji_type}]",
                    "service_name": "CLIENT",
                    "timestamp": time.time(),
                }
            else:

                log = {
                    "log_id": str(uuid.uuid4()),
                    "node_id": NODE_ID,
                    "log_level": "ERROR",
                    "message_type": "LOG",
                    "message": f"[FAILED TO SEND EMOJI {emoji_type}]",
                    "service_name": "CLIENT",
                    "error_details": {
                        "error_code": str(uuid.uuid4()),
                        "error_message": f"[FAILED TO SEND EMOJI {emoji_type}]",
                    },
                    "timestamp": time.time(),
                }

            # time.sleep(1)
            time.sleep(0.05)

        except requests.exceptions.RequestException as e:
            log = {
                "log_id": str(uuid.uuid4()),
                "node_id": NODE_ID,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": f"[FAILED TO SEND EMOJI {emoji_type}]",
                "service_name": "CLIENT",
                "error_details": {
                    "error_code": str(uuid.uuid4()),
                    "error_message": f"[FAILED TO SEND EMOJI {emoji_type}]",
                },
                "timestamp": time.time(),
            }

            print(f"\n[Client {client_id}] Error sending emoji: {e}")
            time.sleep(1)

        # LOGGING LOGIC
        try:

            response = requests.post("http://localhost:5001/client-logs", json=log)

            if response.status_code == 200:
                print("Log Sent")
            else:
                print(f"Failed to send log: {response.status_code}")

            time.sleep(1)

        except KeyboardInterrupt:
            print()
            print("Emoji & Log Generation Stopped!")


def send_hearbeat():
    while True:
        response = requests.post("http://localhost:5001/heartbeat", json=heartbeat)

        if response.status_code == 200:
            print("HEARTBEAT SENT")
        else:
            print(f"Failed to send HEARTBEAT: {response.status_code}")

        time.sleep(3)


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
    print(f"JOINED [{clus}] [{subs}]")
    threading.Thread(target=send_emoji, args=(1,)).start()
    threading.Thread(target=receive_final_data).start()
    threading.Thread(target=send_hearbeat).start()
