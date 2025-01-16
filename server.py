import json
import random
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


NODE_ID = 2

heartbeat = {
    "node_id": NODE_ID,
    "message_type": "HEARTBEAT",
    "status": "UP",
    "timestamp": f"{datetime.now()}",
}

FLUSH_INTERVAL = 0.5


def start_producer_flush(producer, interval):
    def flush_loop():
        while True:
            try:
                producer.flush()
            except Exception as e:
                print(f"Error during flush: {e}")
            time.sleep(interval)

    threading.Thread(target=flush_loop, daemon=True).start()


start_producer_flush(producer, FLUSH_INTERVAL)


@app.route("/emoji", methods=["POST"])
def emoji_server():
    try:
        data = request.get_json()

        producer.send("emoji_topic", value=data)
        print(f"DATA {data}")
        print()

        log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "INFO",
            "message_type": "LOG",
            "message": f"RECIEVED & SENT EMOJI FROM CLIENT [{data['emoji_type']}]",
            "service_name": "SERVER",
            "timestamp": time.time(),
        }

        return jsonify({"status": "emoji_received", "emoji": data["emoji_type"]}), 200
    except Exception as e:

        log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "ERROR",
            "message_type": "LOG",
            "message": f"FAILED TO FORWARD EMOJI [{data['emoji_type']}]",
            "service_name": "SERVER",
            "error_details": {
                "error_code": str(uuid.uuid4()),
                "error_message": "Service Unavailable",
            },
            "timestamp": time.time(),
        }
        print(f"Error sending emoji to Kafka: {e}")
        return jsonify({"error": "Failed to send emoji"}), 500

    try:
        response = requests.post("http://localhost:5002/server-logs", json=log)

        if response.status_code == 200:
            print("Log Sent")
        else:
            print(f"Failed to send log: {response.status_code}")

        time.sleep(1)

    except KeyboardInterrupt:
        print()
        print("Emoji & Log Server Stopped!")


def send_hearbeat():
    while True:
        response = requests.post("http://localhost:5002/heartbeat", json=heartbeat)

        if response.status_code == 200:
            print("HEARTBEAT SENT")
        else:
            print(f"Failed to send HEARTBEAT: {response.status_code}")

        time.sleep(3)


if __name__ == "__main__":
    threading.Thread(target=send_hearbeat).start()
    app.run(debug=True)
