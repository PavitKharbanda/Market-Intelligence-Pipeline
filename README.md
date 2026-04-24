# Market-Intelligence-Pipeline

NewsAPI (HTTP pull)  ─┐
                       ├──► Kafka topics ──► Spark Structured Streaming ──► Delta Lake (bronze)
yfinance (HTTP pull) ─┘
