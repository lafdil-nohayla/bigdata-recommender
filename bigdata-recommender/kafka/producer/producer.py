"""
Kafka Producer — Amazon Fine Food Reviews
Reads Reviews.csv and streams (UserId, ProductId, Score, Time) to Kafka topic.
"""

import os
import time
import json
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC", "food-reviews")
DATA_PATH         = os.getenv("DATA_PATH", "/data/Reviews.csv")
DELAY             = float(os.getenv("DELAY_SECONDS", "0.1"))


def wait_for_kafka(retries: int = 20, wait: int = 5) -> KafkaProducer:
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/%d in %ds …", i + 1, retries, wait)
            time.sleep(wait)
    raise RuntimeError("Cannot connect to Kafka after %d retries" % retries)


def stream_reviews(producer: KafkaProducer) -> None:
    log.info("Loading dataset from %s …", DATA_PATH)

    # Read only needed columns to save memory
    cols = ["UserId", "ProductId", "Score", "Time"]
    df = pd.read_csv(DATA_PATH, usecols=cols).dropna()
    df["Time"] = df["Time"].astype(int)
    df["Score"] = df["Score"].astype(float)

    total = len(df)
    log.info("Dataset loaded: %d reviews — starting stream …", total)

    for idx, row in df.iterrows():
        message = {
            "user_id":    row["UserId"],
            "product_id": row["ProductId"],
            "score":      row["Score"],
            "timestamp":  row["Time"],
        }
        producer.send(TOPIC, value=message)

        if idx % 1000 == 0:
            log.info("Sent %d / %d messages", idx, total)

        time.sleep(DELAY)

    producer.flush()
    log.info("All %d messages published to topic '%s'", total, TOPIC)


if __name__ == "__main__":
    p = wait_for_kafka()
    stream_reviews(p)
