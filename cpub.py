import json
import random
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer

NODE_ID = 1

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
                "message": "Service Running",
                "service_name": "InventoryService",
                "timestamp": time.time(),
            }

        # Send the same data to all cluster topics
        for cluster_topic, producer in producers.items():
            producer.send(cluster_topic, value=data)
            print(f"Sent data to {cluster_topic}: {data}")


if __name__ == "__main__":
    consume_publish()
