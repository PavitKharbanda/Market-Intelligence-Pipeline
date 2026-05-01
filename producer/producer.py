"""
Market Pulse - Kafka Producer
Fetches news (NewsAPI) and price data (yfinance) and publishes to Confluent Kafka.

Two topics:
  news-raw   → JSON blobs of news articles
  prices-raw → JSON blobs of OHLCV price bars per ticker
"""

import json
import time
import logging
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from newsapi import NewsApiClient
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Kafka config ────────────────────────────────────────────────────────────
# SASL_SSL = encrypted connection with username/password auth
# This is how Confluent Cloud secures its public clusters
kafka_config = {
    "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("CONFLUENT_API_KEY"),
    "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
    # How many messages to buffer before forcing a flush
    "queue.buffering.max.messages": 100_000,
}

NEWS_TOPIC = "news-raw"
PRICES_TOPIC = "prices-raw"

# Tickers to track
TICKERS = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "AMD"]

# NewsAPI free tier = 100 req/day. We search by company name, not all tickers.
# Keep this list short or you'll burn your quota.
NEWS_QUERIES = ["NVIDIA stock", "Apple earnings", "Microsoft AI", "stock market"]

producer = Producer(kafka_config)


def delivery_report(err, msg):
    """
    Callback fired after every message is acknowledged by the broker.
    Kafka is async by default — produce() returns immediately, this fires later.
    If delivery fails, err is non-None.
    """
    if err:
        log.error(f"Delivery FAILED | topic={msg.topic()} | error={err}")
    else:
        log.debug(f"Delivered | topic={msg.topic()} | partition={msg.partition()} | offset={msg.offset()}")


def produce(topic: str, key: str, payload: dict):
    """Helper to serialize and send one message."""
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_report,
    )
    # poll(0) is non-blocking — it lets the client handle delivery callbacks
    # without blocking our loop. Without this, callbacks queue up.
    producer.poll(0)


# ─── News ingestion ───────────────────────────────────────────────────────────

def fetch_and_produce_news(newsapi: NewsApiClient):
    """
    Pull top business headlines + a few ticker-specific searches.
    NewsAPI free tier limits: 100 requests/day, 30 days lookback, no full content.
    """
    articles_seen = set()   # deduplicate by URL within this batch
    total = 0

    try:
        # Top business headlines — 1 API call
        result = newsapi.get_top_headlines(category="business", language="en", page_size=20)
        articles = result.get("articles", [])

        # Targeted searches — 1 API call each, keep it to 2-3 max on free tier
        for query in NEWS_QUERIES[:2]:
            result = newsapi.get_everything(
                q=query,
                language="en",
                sort_by="publishedAt",
                page_size=10,
                # Only last 24h so we don't keep re-ingesting old articles
                from_param=(datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d"),
            )
            articles.extend(result.get("articles", []))

        for article in articles:
            url = article.get("url", "")
            title = article.get("title", "")

            # Skip removed articles or duplicates
            if not title or title == "[Removed]" or url in articles_seen:
                continue
            articles_seen.add(url)

            payload = {
                # Ingestion metadata
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                # Article fields
                "source_name": (article.get("source") or {}).get("name"),
                "author": article.get("author"),
                "title": title,
                "description": article.get("description"),
                "url": url,
                "published_at": article.get("publishedAt"),
                "content": article.get("content"),  # truncated to 200 chars on free tier
            }

            # Use URL as message key — Kafka uses keys for partition routing.
            # Same URL always goes to the same partition (consistent hashing).
            produce(NEWS_TOPIC, key=url, payload=payload)
            total += 1

        producer.flush()  # block until all queued messages are delivered
        log.info(f"[News] Produced {total} articles → {NEWS_TOPIC}")

    except Exception as e:
        log.error(f"[News] Fetch failed: {e}")


# ─── Price ingestion ──────────────────────────────────────────────────────────

def fetch_and_produce_prices():
    """
    Pull 1-hour OHLCV bars for each ticker for the last 2 days.
    yfinance scrapes Yahoo Finance — no API key needed, no rate limit concerns at this scale.
    """
    total = 0

    try:
        for ticker in TICKERS:
            stock = yf.Ticker(ticker)
            # period="2d" interval="1h" gives us ~16 bars per ticker (market hours only)
            hist = stock.history(period="2d", interval="1h")

            if hist.empty:
                log.warning(f"[Prices] No data for {ticker} — market may be closed")
                continue

            for ts, row in hist.iterrows():
                payload = {
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "ticker": ticker,
                    "bar_time": ts.isoformat(),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                }

                # Key = ticker + timestamp for deterministic partitioning
                key = f"{ticker}|{ts.isoformat()}"
                produce(PRICES_TOPIC, key=key, payload=payload)
                total += 1

        producer.flush()
        log.info(f"[Prices] Produced {total} bars for {len(TICKERS)} tickers → {PRICES_TOPIC}")

    except Exception as e:
        log.error(f"[Prices] Fetch failed: {e}")


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))
    log.info("Market Pulse producer starting...")

    cycle = 0
    while True:
        cycle += 1
        log.info(f"=== Cycle {cycle} ===")

        fetch_and_produce_news(newsapi)
        fetch_and_produce_prices()

        # 15 minutes between cycles — keeps you within NewsAPI's 100 req/day limit
        # 100 requests / (4 req/cycle) = 25 cycles max per day
        # 24h / 25 = ~58 min between cycles to be safe; 15 min is fine for dev
        log.info("Sleeping 15 minutes...")
        time.sleep(3600)

if __name__ == "__main__":
    main()