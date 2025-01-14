import json
import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, current_timestamp, date_format,
                                   first, from_json, from_unixtime, lit, max,
                                   struct, window)
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)

spark = (
    SparkSession.builder.appName("EmojiConsumer")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .getOrCreate()
)

emoji_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "emoji_topic")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = df.withColumn(
    "value", from_json(col("value").cast("string"), emoji_schema)
).select(
    col("value.user_id").alias("user_id"),
    col("value.emoji_type").alias("emoji_type"),
    from_unixtime(col("value.timestamp").cast("long"))
    .cast(TimestampType())
    .alias("timestamp"),
    lit(1).alias("count"),
)

windowed_df = (
    parsed_df.withWatermark("timestamp", "1 seconds")
    .groupBy(window(col("timestamp"), "2 seconds", "2 seconds"), col("emoji_type"))
    .agg(count("count").alias("emoji_count"))
)


def process_window_batch(df, epoch_id):
    if df.isEmpty():
        return

    # Find max emoji for each window
    max_emoji_df = df.groupBy("window").agg(
        first(col("emoji_type"), ignorenulls=True).alias("max_emoji"),
        max("emoji_count").alias("max_count"),
    )

    batch_start_time = time.time()
    while True:
        # Convert to list of dicts
        data_to_send = [row.asDict() for row in max_emoji_df.collect()]

        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode(
                "utf-8"
            ),
        )

        for row_data in data_to_send:
            # Add window start and end times for clarity
            row_data["window_start"] = row_data["window"]["start"].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            row_data["window_end"] = row_data["window"]["end"].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            del row_data["window"]
            producer.send("main_topic", value=row_data)

        producer.flush()
        producer.close()
        time_elapsed = time.time() - batch_start_time
        if time_elapsed < 2:
            time.sleep(2 - time_elapsed)
        break


query = (
    windowed_df.writeStream.outputMode("append")
    .foreachBatch(process_window_batch)
    .start()
)

query.awaitTermination()
