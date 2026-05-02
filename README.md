# Market Pulse

Real-time market intelligence pipeline with an agentic AI layer. Ingests financial news and equity price data through Confluent Kafka, processes through a Delta Lake medallion architecture, and exposes a LangGraph RAG agent via FastAPI and Streamlit.

**Live Demo:** https://market-pulse-frontend-846454581852.us-central1.run.app  
**API Docs:** https://market-pulse-api-846454581852.us-central1.run.app/docs

---

## Architecture

```
NewsAPI + yfinance
        ↓
Confluent Kafka (news-raw, prices-raw topics)
        ↓
Spark Structured Streaming (micro-batch, 30s trigger)
        ↓
Delta Lake Bronze (raw, append-only)
        ↓  [silver/silver_transform.py — PySpark batch]
Delta Lake Silver (deduplicated, typed, cleaned)
        ↓                          ↓
FAISS Vector Index          dbt Gold Models
(news embeddings)           (anomaly flags, daily sentiment)
        ↓                          ↓
        └──────── LangGraph Agent ─┘
                  (3 tools: RAG, gold query, live price)
                        ↓
                   FastAPI /query
                        ↓
                  Streamlit Chat UI
                        ↓
                  GCP Cloud Run
```

**Orchestration:** Airflow DAG (silver → dbt gold, weekdays 6am ET)

---

## Screenshots

**Spark Structured Streaming UI** — 2 active streaming queries (news + prices) consuming from Confluent Kafka:

![Spark UI showing 2 active streaming queries (news + prices)](image.png)

---

## Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Confluent Kafka (managed), Python producer |
| Stream processing | Spark Structured Streaming 3.5.1 |
| Storage | Delta Lake (bronze/silver/gold medallion) |
| Batch transforms | PySpark, dbt |
| Orchestration | Airflow (Astro CLI) |
| Vector store | FAISS (IndexFlatIP, cosine similarity) |
| Embeddings | OpenAI text-embedding-3-small (1536 dims) |
| Agent | LangGraph ReAct agent, GPT-4o-mini |
| Backend | FastAPI + uvicorn |
| Frontend | Streamlit |
| Containerization | Docker Compose |
| CI/CD | GitHub Actions |
| Deployment | GCP Cloud Run |

---

## Project Structure

```
market-pulse/
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py              # Confluent Kafka producer (NewsAPI + yfinance)
├── spark/
│   └── streaming_consumer.py    # Spark Structured Streaming → Delta Lake bronze
├── silver/
│   └── silver_transform.py      # PySpark batch: bronze → silver (dedup, clean, type)
├── dbt/
│   └── market_pulse/
│       ├── dbt_project.yml
│       └── models/
│           ├── gold_daily_sentiment.sql       # article count per day
│           └── gold_price_anomaly_flags.sql   # z-score volume/range anomaly detection
├── agent/
│   ├── embedder.py              # OpenAI embeddings → FAISS index
│   ├── tools.py                 # 3 LangGraph tools: RAG, gold query, live price
│   ├── graph.py                 # LangGraph ReAct agent
│   ├── test_agent.py            # local test script
│   └── faiss_index/             # persisted FAISS index (gitignored)
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                  # FastAPI: /health, /query, /index-stats
├── frontend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                   # Streamlit chat UI
├── airflow/
│   └── dags/
│       └── market_pulse_dag.py  # Airflow DAG: silver → dbt gold (weekdays 6am)
├── delta_lake/                  # local Delta Lake storage (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── .env                         # secrets (gitignored)
└── docker-compose.yml
```

---

## Local Development

### Prerequisites
- Docker + OrbStack (Mac) or Docker Desktop
- Python 3.9+
- Java 11+ (for PySpark)
- Astro CLI (for Airflow)

### Environment Variables

Create `.env` in project root:

```bash
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
CONFLUENT_API_KEY=your_key
CONFLUENT_API_SECRET=your_secret
NEWS_API_KEY=your_newsapi_key
OPENAI_API_KEY=sk-proj-xxxxx
```

### Running the full stack locally

```bash
# 1. Start streaming pipeline (producer + Spark)
docker-compose up

# 2. Run silver transform (after bronze has data)
docker-compose --profile silver run --rm spark-silver

# 3. Run dbt gold models (start thrift server first)
docker-compose --profile dbt up spark-thrift -d
cd dbt/market_pulse && dbt run

# 4. Build FAISS index
source producer/venv/bin/activate
python -m agent.embedder

# 5. Start API + frontend
docker-compose up api frontend
```

Open `http://localhost:8501` for the chat UI.
Open `http://localhost:8081/docs` for the Swagger API explorer.

### Airflow (Astro CLI)

```bash
cd airflow
astro dev start
# UI at http://localhost:8080 (admin/admin)
```

---

## Agent Tools

The LangGraph agent has three tools:

**1. `rag_search(query)`**
Embeds the query using OpenAI text-embedding-3-small, searches FAISS index (IndexFlatIP, cosine similarity), returns top 5 most relevant news articles.

**2. `query_gold_table(table_name)`**
Reads Delta Lake gold tables via PySpark. Available tables:
- `price_anomaly_flags` — volume and price range z-scores per ticker, anomaly boolean flag
- `daily_sentiment` — article count per day, source diversity, attention proxy score

**3. `get_live_price(ticker)`**
Fetches current OHLCV data via yfinance. Returns latest close, daily change, volume, 52-week range.

---

## Data Pipeline Details

### Bronze layer
Raw, unmodified data from Kafka. Never deleted. Two tables:
- `bronze/news` — raw article JSON with Kafka metadata
- `bronze/prices` — raw OHLCV bars with Kafka metadata

### Silver layer
Cleaned and deduplicated. PySpark batch job:
- News: dedup by URL (keep earliest), drop null titles, parse timestamps
- Prices: dedup by (ticker, bar_time), add price_range column, parse timestamps

### Gold layer (dbt)
Analytics-ready aggregations:
- `gold_daily_sentiment` — article count, source diversity, attention proxy per day
- `gold_price_anomaly_flags` — 20-bar rolling z-scores for volume and price range, `is_anomaly` flag when z-score exceeds 2.0

### FAISS Index
- Model: OpenAI text-embedding-3-small (1536 dims)
- Index type: IndexFlatIP (exact search, cosine similarity via inner product on normalized vectors)
- Refresh: manually via `python -m agent.embedder` (Airflow task planned)

---

## Key Technical Decisions

**Why Kafka over direct API polling in Spark?**
Decoupling. Producer and consumer are independent. Kafka buffers messages if Spark restarts. Replay from any offset. Can add consumers without touching producer.

**Why Delta Lake over plain Parquet?**
ACID transactions, schema enforcement, time travel, transaction log gives exactly-once guarantees with Spark checkpointing.

**Why PySpark for silver, dbt for gold?**
Silver needs programmatic logic (dedup by URL, null handling, type coercion) — PySpark. Gold is SQL aggregations — exactly what dbt is designed for.

**Why LangGraph over plain LangChain AgentExecutor?**
LangGraph manages the ReAct loop as a proper state machine with conditional edges. Inspectable at each step, easier to debug, supports human-in-the-loop.

**Why GPT-4o-mini?**
Cost and speed. For tool-calling with structured outputs, performs comparably to GPT-4o at a fraction of the cost. Temperature=0 for deterministic answers.

**Why IndexFlatIP over approximate FAISS index?**
Exact search is appropriate for hundreds to low thousands of articles. For millions of articles, switch to IndexIVFFlat.

---

## Deployment (GCP Cloud Run)

```bash
# Build for linux/amd64 (Cloud Run is x86)
docker build --platform linux/amd64 \
  -t us-central1-docker.pkg.dev/market-pulse-ai-10/market-pulse/api:latest \
  -f api/Dockerfile .

docker push us-central1-docker.pkg.dev/market-pulse-ai-10/market-pulse/api:latest

gcloud run deploy market-pulse-api \
  --image us-central1-docker.pkg.dev/market-pulse-ai-10/market-pulse/api:latest \
  --platform managed --region us-central1 \
  --allow-unauthenticated --memory 4Gi --cpu 2 --timeout 300 \
  --set-env-vars OPENAI_API_KEY=sk-proj-xxxxx,DELTA_ROOT=/delta_lake
```

---

## Metrics

- Articles indexed in FAISS: 129 (grows with each pipeline refresh)
- Tickers tracked: 8 (NVDA, AAPL, MSFT, GOOGL, AMZN, TSLA, META, AMD)
- Agent latency: 4-10s (RAG queries), 10-20s (gold table queries, Spark startup overhead)
- Producer cycle: every 15 min during market hours (4 API calls/cycle, under NewsAPI 100 req/day limit)
- dbt models: 2 gold (gold_daily_sentiment, gold_price_anomaly_flags)
- Airflow schedule: 0 6 * * 1-5 (6am ET weekdays)
- Delta Lake: exactly-once guarantees via Spark Structured Streaming checkpointing

---

## Potential Improvements

- MLflow experiment tracking for embedder runs (article count, embedding cost, index size)
- Prometheus metrics on FastAPI (request count, latency histogram, error rate)
- Airflow task to re-embed on schedule (currently manual)
- Sentiment scoring via OpenAI → write back to gold_news_sentiment table
- Switch FAISS to IndexIVFFlat when article count exceeds 10,000
- Ticker extraction from articles (NER) to link news to specific stocks
- WebSocket streaming for agent responses
- Redis caching layer for identical queries (5 min TTL)
- Replace yfinance with Polygon.io for more reliable market data