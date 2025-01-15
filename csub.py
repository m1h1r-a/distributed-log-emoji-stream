import json
import random
import sys

from kafka import KafkaConsumer, KafkaProducer

SUBSCRIBERS = [
    "s1",
    "s2",
    "s3",
]

CLUSTERS = ["c1", "c2", "c3"]

# CLIENT_TOPIC = random.choice(CLUSTERS)
CLIENT_TOPIC = sys.argv[1]


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
            topic = CLIENT_TOPIC + cluster_topic
            producer.send(topic, value=data)
            print(f"[SENT {topic}] [{data}]")


if __name__ == "__main__":
    consume_publish()
