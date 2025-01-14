import json
import sys

from kafka import KafkaConsumer, KafkaProducer

# Cluster topic to listen from
CLIENT_TOPIC = sys.argv[1]


SUBSCRIBERS = [
    "s1",
    "s2",
    "s3",
]


def consume_publish():
    # Kafka consumer to read from the main topic
    consumer = KafkaConsumer(
        CLIENT_TOPIC,
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
        for cluster in SUBSCRIBERS
    }

    # Iterate through messages from the main topic
    for message in consumer:
        data = message.value  # Get the message value (processed data)
        print(f"Received data from cluster: {data}")

        # Send the same data to all cluster topics
        for cluster_topic, producer in producers.items():
            producer.send(cluster_topic, value=data)
            print(f"Sent data to {cluster_topic}: {data}")


if __name__ == "__main__":
    consume_publish()
