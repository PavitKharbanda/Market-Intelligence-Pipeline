# Market-Intelligence-Pipeline

NewsAPI (HTTP pull)  ─┐
                       ├──► Kafka topics ──► Spark Structured Streaming ──► Delta Lake (bronze)
yfinance (HTTP pull) ─┘


market-pulse/
├── .env                          # secrets, never commit this
├── .gitignore
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── spark/
│   └── streaming_consumer.py     # no Dockerfile needed, using bitnami image directly
├── delta_lake/                   # auto-created, gitignored
│   ├── bronze/
│   │   ├── news/
│   │   └── prices/
│   └── checkpoints/
└── README.md


![Spark UI showing 2 active streaming queries (news + prices)](image.png)



market-pulse/
├── silver/
│   └── silver_transform.py       # PySpark batch job
├── dbt/
│   ├── profiles.yml               # dbt connection config
│   ├── dbt_project.yml
│   └── models/
│       ├── gold_daily_sentiment.sql
│       └── gold_price_anomaly_flags.sql
├── airflow/                       # astro project lives here
│   ├── dags/
│   │   └── market_pulse_dag.py
│   └── (astro-generated files)


silver/news Delta table
        ↓
embedder.py: reads articles, calls OpenAI embeddings API, 
             saves FAISS index to agent/faiss_index/
        ↓
agent/tools.py: defines 3 tools
  Tool 1 - rag_search(query): 
    embeds query → searches FAISS → returns top 5 articles
  Tool 2 - query_gold_table(sql):
    reads Delta Lake gold tables directly with pandas
    returns anomaly flags, daily sentiment counts
  Tool 3 - get_live_price(ticker):
    calls yfinance for current price
        ↓
agent/graph.py: LangGraph agent
  User query → LLM (GPT-4o-mini) decides which tools to call
  → calls tools → gets results → synthesizes final answer
        ↓
FastAPI endpoint (Phase 4): wraps graph.py in an HTTP endpoint
Streamlit frontend (Phase 4): chat UI that calls the endpoint