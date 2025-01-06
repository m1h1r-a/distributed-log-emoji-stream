import json
import threading
import time

import requests
from flask import Flask, jsonify, render_template, request
from kafka import KafkaProducer

app = Flask(__name__)


@app.route("/emoji", methods=["POST"])
def emoji_server():
    try:
        data = request.get_json()
        print(f"DATA {data}")
        print()

        return jsonify({"status": "emoji_received", "emoji": data["emoji_type"]}), 200
    except Exception as e:
        print(f"Error sending emoji to Kafka: {e}")
        return jsonify({"error": "Failed to send emoji"}), 500


if __name__ == "__main__":
    app.run(debug=True)
