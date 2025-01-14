import json
import threading
import time

import requests
from flask import Flask, jsonify, render_template, request
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


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

        return jsonify({"status": "emoji_received", "emoji": data["emoji_type"]}), 200
    except Exception as e:
        print(f"Error sending emoji to Kafka: {e}")
        return jsonify({"error": "Failed to send emoji"}), 500


if __name__ == "__main__":
    app.run(debug=True)
