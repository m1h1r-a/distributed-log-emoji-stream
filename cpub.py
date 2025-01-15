import json
import random
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer

NODE_ID = 3

heartbeat = {
    "node_id": NODE_ID,
    "message_type": "HEARTBEAT",
    "status": "UP",
    "timestamp": f"{datetime.now()}",
}

CLUSTERS = [
    "c1",
    "c2",
    "c3",
]


def consume_publish():
    # Kafka consumer to read from the main topic
    consumer = KafkaConsumer(
        "main_topic",
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    # Kafka producers for each cluster topic
    producers = {
        cluster: KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode(
                "utf-8"
            ),
        )
        for cluster in CLUSTERS
    }

    # Iterate through messages from the main topic
    for message in consumer:
        try:
            data = message.value  # Get the message value (processed data)
            print(f"Received data from main_topic: {data}")

            log = {
                "log_id": str(uuid.uuid4()),
                "node_id": NODE_ID,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": f"[PUBLISHED EMOJI {data['emoji_type']}]",
                "service_name": "PUBLISHER",
                "timestamp": time.time(),
            }

        except:
            log = {
                "log_id": str(uuid.uuid4()),
                "node_id": NODE_ID,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": f"[FAILED TO PUBLISH EMOJI {data['emoji_type']}]",
                "service_name": "PUBLISHER",
                "error_details": {
                    "error_code": str(uuid.uuid4()),
                    "error_message": f"[FAILED TO SEND EMOJI {data['emoji_type']}]",
                },
                "timestamp": time.time(),
            }

        try:
            response = requests.post("http://localhost:5003/cpub-logs", json=log)

            if response.status_code == 200:
                print("Log Sent")
            else:
                print(f"Failed to send log: {response.status_code}")

            time.sleep(1)

        except:
            print()
            print("LOG PUBLSHING STOPPED!")

        # Send the same data to all cluster topics
        for cluster_topic, producer in producers.items():
            producer.send(cluster_topic, value=data)
            print(f"Sent data to {cluster_topic}: {data}")


def send_hearbeat():
    while True:
        response = requests.post("http://localhost:5003/heartbeat", json=heartbeat)

        if response.status_code == 200:
            print("HEARTBEAT SENT")
        else:
            print(f"Failed to send HEARTBEAT: {response.status_code}")

        time.sleep(3)


if __name__ == "__main__":
    threading.Thread(target=send_hearbeat).start()
    consume_publish()
