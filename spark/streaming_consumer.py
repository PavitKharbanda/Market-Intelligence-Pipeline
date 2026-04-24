"""
Market Pulse - Spark Structured Streaming Consumer
Reads raw messages from Kafka topics and writes them to Delta Lake bronze tables.

Bronze layer = raw, unmodified data + ingestion metadata.
We never transform or filter here. If bad data comes in, bronze still has it.
Transformation happens at silver layer (Phase 2).

Two streaming queries run concurrently in the same Spark session:
  Kafka(news-raw)    → delta_lake/bronze/news
  Kafka(prices-raw)  → delta_lake/bronze/prices
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")

# Where Delta tables land inside the container
# This path is volume-mounted to ./delta_lake on your host machine
DELTA_ROOT = "/delta_lake"

# ─── JAAS config ──────────────────────────────────────────────────────────────
# JAAS = Java Authentication and Authorization Service
# Spark uses Java's Kafka client under the hood, so auth config is in JAAS format.
# This is the standard way to pass SASL credentials to Spark's Kafka connector.
JAAS = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{API_KEY}" password="{API_SECRET}";'
)

# Options shared by both streaming readers
# All Spark kafka options must be prefixed with "kafka." — Spark passes these
# directly to the underlying Kafka consumer, stripping the prefix.
KAFKA_READ_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": JAAS,
    "kafka.ssl.endpoint.identification.algorithm": "https",
    # latest = only process new messages from when this job starts
    # earliest = reprocess everything from the beginning of the topic
    # Use "earliest" if you want to backfill; "latest" for normal operation
    "startingOffsets": "latest",
    # Don't crash if Kafka deletes old segments before we read them
    "failOnDataLoss": "false",
}


# ─── Schemas ─────────────────────────────────────────────────────────────────
# Kafka delivers raw bytes. We need to tell Spark what the JSON structure is.
# from_json() uses these schemas to parse value bytes → structured columns.
# If a field is missing in the JSON, Spark returns null (doesn't crash).

NEWS_SCHEMA = StructType([
    StructField("ingested_at", StringType()),
    StructField("source_name", StringType()),
    StructField("author", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("url", StringType()),
    StructField("published_at", StringType()),
    StructField("content", StringType()),
])

PRICE_SCHEMA = StructType([
    StructField("ingested_at", StringType()),
    StructField("ticker", StringType()),
    StructField("bar_time", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
])


# ─── Spark session ────────────────────────────────────────────────────────────

def build_spark():
    return (
        SparkSession.builder
        .appName("MarketPulse-Bronze-Writer")
        # These two lines wire in the Delta Lake extensions.
        # DeltaSparkSessionExtension adds Delta-specific SQL commands (VACUUM, DESCRIBE HISTORY etc.)
        # DeltaCatalog makes spark.table("bronze.news") resolve to Delta tables
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Micro-batch trigger interval — process whatever arrived in the last 30s
        # Lower = more latency sensitivity but more overhead; 30s is fine for news/prices
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


# ─── Stream builders ─────────────────────────────────────────────────────────

def read_topic(spark: SparkSession, topic: str):
    """
    Returns a streaming DataFrame.
    Kafka gives us columns: key, value, topic, partition, offset, timestamp, timestampType
    value is raw bytes — we'll parse it with from_json downstream.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("subscribe", topic)
        .options(**KAFKA_READ_OPTIONS)
        .load()
    )


def news_stream(spark: SparkSession):
    """
    Kafka message bytes → structured news DataFrame → Delta bronze
    """
    raw = read_topic(spark, "news-raw")

    parsed = (
        raw
        # value is BinaryType in Kafka — cast to string first
        .withColumn("json_str", col("value").cast("string"))
        # from_json parses the JSON string into a struct column using our schema
        .withColumn("data", from_json(col("json_str"), NEWS_SCHEMA))
        # Expand the struct into top-level columns
        .select(
            col("data.ingested_at"),
            col("data.source_name"),
            col("data.author"),
            col("data.title"),
            col("data.description"),
            col("data.url"),
            col("data.published_at"),
            col("data.content"),
            # Kafka metadata — useful for debugging offset lag
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            # When Spark processed this message
            current_timestamp().alias("spark_written_at"),
        )
    )

    return (
        parsed.writeStream
        .format("delta")
        .outputMode("append")       # append = never update/delete, only add rows
        # Checkpoint = Spark's WAL (write-ahead log) for exactly-once guarantees
        # Stores which Kafka offsets have been committed to Delta.
        # If Spark crashes and restarts, it resumes from the last checkpoint.
        # NEVER share a checkpoint dir between two streams.
        .option("checkpointLocation", f"{DELTA_ROOT}/checkpoints/news_bronze")
        .start(f"{DELTA_ROOT}/bronze/news")
    )


def prices_stream(spark: SparkSession):
    """
    Kafka message bytes → structured prices DataFrame → Delta bronze
    """
    raw = read_topic(spark, "prices-raw")

    parsed = (
        raw
        .withColumn("json_str", col("value").cast("string"))
        .withColumn("data", from_json(col("json_str"), PRICE_SCHEMA))
        .select(
            col("data.ingested_at"),
            col("data.ticker"),
            col("data.bar_time"),
            col("data.open"),
            col("data.high"),
            col("data.low"),
            col("data.close"),
            col("data.volume"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            current_timestamp().alias("spark_written_at"),
        )
    )

    return (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{DELTA_ROOT}/checkpoints/prices_bronze")
        .start(f"{DELTA_ROOT}/bronze/prices")
    )


# ─── Entry point ─────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")   # suppress INFO spam from Spark internals

    print("Starting bronze writers...")

    q1 = news_stream(spark)
    q2 = prices_stream(spark)

    print(f"News stream: {q1.status}")
    print(f"Prices stream: {q2.status}")
    print("Streaming... check localhost:4040 for Spark UI")

    # Block until either stream fails or is manually stopped
    # If one crashes, awaitAnyTermination unblocks so you can see the exception
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()