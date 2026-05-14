"""
Spark Structured Streaming — Real-time Recommendation Generator
───────────────────────────────────────────────────────────────
Consumes (user_id, product_id, score) from Kafka topic,
loads the pre-trained ALS model, generates Top-N recommendations
per user batch, and writes results to PostgreSQL.
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.ml.recommendation import ALSModel
from pyspark.ml import PipelineModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",             "food-reviews")
MODEL_PATH    = os.getenv("MODEL_PATH",               "/opt/spark-models/als_model")
INDEXER_PATH  = os.getenv("INDEXER_PATH",             "/opt/spark-models/als_model_indexer")
DB_URL        = os.getenv("DB_URL",  "jdbc:postgresql://postgres:5432/recommender")
DB_USER       = os.getenv("DB_USER", "airflow")
DB_PASS       = os.getenv("DB_PASS", "airflow")
TOP_N         = int(os.getenv("TOP_N", "10"))

REVIEW_SCHEMA = StructType([
    StructField("user_id",    StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("score",      FloatType(),  True),
    StructField("timestamp",  LongType(),   True),
])


def build_spark() -> SparkSession:
    packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.6.0",
    ])
    return (
        SparkSession.builder
        .appName("ALS-Streaming-Recommender")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


def write_recommendations(df, epoch_id):
    """Foreach-batch handler: generate recommendations and persist to PG."""
    if df.rdd.isEmpty():
        return

    spark = df.sparkSession

    # Load models (lazy, cached after first load)
    als_model     = ALSModel.load(MODEL_PATH)
    indexer_model = PipelineModel.load(INDEXER_PATH)

    # Encode incoming users via saved indexer
    df_enc = indexer_model.transform(
        df.select("user_id", "product_id", "score")
    )
    df_enc = (
        df_enc
        .withColumn("user_idx",    F.col("user_idx").cast("integer"))
        .withColumn("product_idx", F.col("product_idx").cast("integer"))
    )

    # Distinct users in this micro-batch
    users = df_enc.select("user_idx", "user_id").distinct()

    # ALS recommendForUserSubset
    recs = als_model.recommendForUserSubset(users, TOP_N)

    # Explode recommendations
    recs_flat = (
        recs
        .join(users, "user_idx")
        .select("user_id", F.explode("recommendations").alias("rec"))
        .select(
            "user_id",
            F.col("rec.product_idx").alias("product_idx"),
            F.col("rec.rating").alias("predicted_rating"),
        )
    )

    # Resolve product index → product_id string via indexer metadata
    # We join back using a broadcast lookup built from the indexer labels
    user_stage    = indexer_model.stages[0]
    product_stage = indexer_model.stages[1]
    product_labels = product_stage.labels  # array of original ProductId strings

    product_lookup = spark.createDataFrame(
        [(i, pid) for i, pid in enumerate(product_labels)],
        schema=["product_idx", "product_id_orig"],
    )

    recs_named = (
        recs_flat
        .join(F.broadcast(product_lookup), "product_idx")
        .select("user_id", "product_id_orig", "predicted_rating")
        .withColumnRenamed("product_id_orig", "product_id")
        .withColumn("epoch_id", F.lit(int(epoch_id)))
    )

    # Write to PostgreSQL
    (
        recs_named.write
        .format("jdbc")
        .option("url",      DB_URL)
        .option("dbtable",  "recommendations")
        .option("user",     DB_USER)
        .option("password", DB_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    log.info("[epoch %d] Wrote recommendations for %d users",
             epoch_id, users.count())


def run():
    spark = build_spark()

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), REVIEW_SCHEMA).alias("data"))
        .select("data.*")
        .dropna()
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_recommendations)
        .option("checkpointLocation", "/opt/spark-models/checkpoints/streaming")
        .trigger(processingTime="60 seconds")
        .start()
    )

    log.info("Streaming query started — waiting for data …")
    query.awaitTermination()


if __name__ == "__main__":
    run()
