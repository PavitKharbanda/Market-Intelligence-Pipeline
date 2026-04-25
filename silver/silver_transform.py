"""
Market Pulse - Silver Layer Transformation
Reads bronze Delta tables, cleans and deduplicates, writes to silver Delta.

Bronze → Silver rules:
  news:   deduplicate by url, drop rows missing title/url, parse timestamps
  prices: deduplicate by (ticker, bar_time), drop rows missing close, parse timestamps

Run this as a batch job (triggered by Airflow).
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from pyspark.sql.window import Window
import pyspark.sql.functions as F

DELTA_ROOT = "/delta_lake"

def build_spark():
    return (
        SparkSession.builder
        .appName("MarketPulse-Silver-Transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def transform_news(spark: SparkSession):
    """
    Bronze news → Silver news

    Steps:
    1. Read bronze Delta
    2. Drop rows where title or url is null (unusable records)
    3. Deduplicate by url — same article can arrive multiple times if producer
       runs overlapping cycles. We keep the first-seen record using a window
       function ranked by kafka_timestamp.
    4. Parse published_at string → proper TimestampType
    5. Add silver_processed_at metadata column
    6. Write to silver/news in Delta format, overwrite each run
       (idempotent — safe to rerun if something fails)
    """
    print("Reading bronze/news...")
    bronze = spark.read.format("delta").load(f"{DELTA_ROOT}/bronze/news")
    print(f"Bronze news row count: {bronze.count()}")

    # Step 1: drop rows missing critical fields
    cleaned = bronze.filter(
        col("title").isNotNull() &
        col("url").isNotNull() &
        (col("title") != "[Removed]")
    )

    # Step 2: deduplicate by url, keeping the earliest kafka_timestamp
    # Window: partition by url, order by kafka_timestamp ascending
    # row_number() = 1 means "first time we saw this url"
    window = Window.partitionBy("url").orderBy("kafka_timestamp")
    deduped = (
        cleaned
        .withColumn("rn", F.row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Step 3: parse timestamps
    # published_at comes in as ISO string e.g. "2026-04-24T03:45:55Z"
    # to_timestamp with format handles both Z and +00:00 suffixes
    result = (
        deduped
        .withColumn(
            "published_at_ts",
            to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .withColumn(
            "ingested_at_ts",
            to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        .withColumn("silver_processed_at", current_timestamp())
    )

    # Step 4: write to silver — overwrite mode makes this idempotent
    print("Writing silver/news...")
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{DELTA_ROOT}/silver/news")
    )

    count = spark.read.format("delta").load(f"{DELTA_ROOT}/silver/news").count()
    print(f"Silver news written: {count} rows")


def transform_prices(spark: SparkSession):
    """
    Bronze prices → Silver prices

    Steps:
    1. Drop rows where close is null
    2. Deduplicate by (ticker, bar_time) — same bar can arrive in multiple
       producer cycles since yfinance returns the last N bars each call
    3. Parse bar_time string → TimestampType
    4. Add derived column: price_range = high - low (useful for anomaly detection later)
    5. Write to silver/prices
    """
    print("Reading bronze/prices...")
    bronze = spark.read.format("delta").load(f"{DELTA_ROOT}/bronze/prices")
    print(f"Bronze prices row count: {bronze.count()}")

    # Drop nulls on fields we need for gold layer calculations
    cleaned = bronze.filter(
        col("close").isNotNull() &
        col("ticker").isNotNull() &
        col("bar_time").isNotNull()
    )

    # Deduplicate by (ticker, bar_time) — keep earliest ingestion
    window = Window.partitionBy("ticker", "bar_time").orderBy("kafka_timestamp")
    deduped = (
        cleaned
        .withColumn("rn", F.row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    result = (
        deduped
        .withColumn(
            "bar_time_ts",
            to_timestamp(col("bar_time"))
        )
        .withColumn(
            "ingested_at_ts",
            to_timestamp(col("ingested_at"), "yyyy-MM-dd'T'HH:mm:ssXXX")
        )
        # price_range is a simple volatility proxy — used in gold anomaly model
        .withColumn("price_range", col("high") - col("low"))
        .withColumn("silver_processed_at", current_timestamp())
    )

    print("Writing silver/prices...")
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{DELTA_ROOT}/silver/prices")
    )

    count = spark.read.format("delta").load(f"{DELTA_ROOT}/silver/prices").count()
    print(f"Silver prices written: {count} rows")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    transform_news(spark)
    transform_prices(spark)

    print("Silver transformation complete.")
    spark.stop()


if __name__ == "__main__":
    main()