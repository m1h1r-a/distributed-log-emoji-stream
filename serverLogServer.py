import json

from flask import Flask, request
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode("utf-8"))


@app.route("/server-logs", methods=["POST"])
def receive_logs():
    log = request.json

    # Process and forward the log to the Pub-Sub Model
    if log["log_level"] == "INFO":
        producer.send("info_log", log)
        producer.flush()
        print(f"Received & Sent log: \n{json.dumps(log,indent = 4)}")

    elif log["log_level"] == "ERROR":
        producer.send("error_log", log)
        producer.flush()
        print(f"Received & Sent log: \n{json.dumps(log,indent = 4)}")

    return "Log received", 200


@app.route("/heartbeat", methods=["POST"])
def receive_heartbeat():
    heartbeat = request.json

    producer.send("serverHeartbeat", request.json)
    producer.flush()

    print(
        "--------------------------------RECEIVED HEARTBEAT--------------------------------"
    )

    return "Hearbeat received", 200


if __name__ == "__main__":
    app.run(port=5002)
