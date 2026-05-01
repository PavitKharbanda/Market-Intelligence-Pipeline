"""
Market Pulse - Agent Tools

Three tools the LangGraph agent can invoke:

1. rag_search        -- semantic search over news FAISS index
2. query_gold_table  -- SQL queries against Delta Lake gold tables
3. get_live_price    -- real-time price lookup via yfinance

Each tool has a docstring that the LLM reads to decide when to use it.
Write docstrings clearly -- they are literally the LLM's instructions.
"""

import os
import pickle
import warnings
from pathlib import Path

import faiss
import numpy as np
import yfinance as yf
import pandas as pd
from openai import OpenAI
from langchain_core.tools import tool
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

DELTA_ROOT = os.getenv("DELTA_ROOT", "./delta_lake")
INDEX_DIR = Path("agent/faiss_index")
INDEX_PATH = INDEX_DIR / "news.index"
METADATA_PATH = INDEX_DIR / "metadata.pkl"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536
TOP_K = 5   # number of articles to retrieve per RAG search


# ── Lazy-loaded singletons ────────────────────────────────────────────────────
# We load the FAISS index and OpenAI client once at module load time,
# not on every tool call. This avoids re-reading the index from disk
# on every agent invocation.

_client = None
_faiss_index = None
_metadata = None


def get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client


def get_faiss_index():
    """Load FAISS index and metadata from disk (once)."""
    global _faiss_index, _metadata

    if _faiss_index is None:
        if not INDEX_PATH.exists():
            raise FileNotFoundError(
                f"FAISS index not found at {INDEX_PATH}. "
                "Run agent/embedder.py first."
            )
        _faiss_index = faiss.read_index(str(INDEX_PATH))

    if _metadata is None:
        with open(METADATA_PATH, "rb") as f:
            _metadata = pickle.load(f)

    return _faiss_index, _metadata


def build_spark_for_query():
    """
    Build a minimal Spark session for batch Delta reads.
    We use SparkSession for gold table queries because Delta Lake
    requires it -- Delta files are Parquet + transaction log,
    and Spark is the engine that knows how to read them consistently.
    """
    
    return (
        SparkSession.builder
        .appName("MarketPulse-Agent-Query")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ── Tool 1: RAG Search ────────────────────────────────────────────────────────

@tool
def rag_search(query: str) -> str:
    """
    Search recent financial news articles using semantic similarity.
    Use this tool when the user asks about recent news, market drivers,
    company events, analyst opinions, or anything requiring current information.
    
    Input: a natural language search query like 'NVIDIA AI chip demand' 
           or 'Federal Reserve interest rate decision'
    Output: top 5 most relevant recent news articles with title, source, and summary
    """
    client = get_client()
    index, metadata = get_faiss_index()

    # Embed the query using the same model used to embed articles
    # Critical: must use identical model -- mismatched models give garbage results
    response = client.embeddings.create(
        input=[query],
        model=EMBEDDING_MODEL,
    )
    query_vector = np.array([response.data[0].embedding], dtype=np.float32)
    faiss.normalize_L2(query_vector)

    # Search: returns distances and indices of top-k nearest vectors
    # distances[i] = cosine similarity score (0 to 1, higher = more similar)
    # indices[i] = position in metadata list
    distances, indices = index.search(query_vector, TOP_K)

    if len(indices[0]) == 0:
        return "No relevant articles found."

    results = []
    for rank, (idx, score) in enumerate(zip(indices[0], distances[0]), 1):
        if idx == -1:   # FAISS returns -1 for empty slots
            continue
        article = metadata[idx]
        results.append(
            f"[{rank}] {article['title']}\n"
            f"    Source: {article.get('source_name', 'Unknown')} | "
            f"Published: {article.get('published_at', 'Unknown')} | "
            f"Relevance: {score:.3f}\n"
            f"    {article['text'][:200]}..."
        )

    return "\n\n".join(results) if results else "No relevant articles found."


# ── Tool 2: Gold Table Query ──────────────────────────────────────────────────

@tool
def query_gold_table(table_name: str) -> str:
    """
    Query Delta Lake gold tables for structured market analytics.
    Use this tool to check for volume anomalies, price anomaly flags,
    or daily sentiment/attention metrics.
    
    Available tables:
    - 'price_anomaly_flags': contains ticker, bar_time_ts, volume, 
      volume_zscore, range_zscore, is_anomaly columns.
      Use to answer questions about unusual trading activity.
    - 'daily_sentiment': contains published_date, article_count, 
      source_diversity, attention_score_proxy columns.
      Use to answer questions about news volume trends.
    
    Input: table name string, either 'price_anomaly_flags' or 'daily_sentiment'
    Output: latest rows from the table as formatted text
    """
    valid_tables = {
    "price_anomaly_flags": f"{DELTA_ROOT}/gold/gold_price_anomaly_flags",
    "daily_sentiment": f"{DELTA_ROOT}/gold/gold_daily_sentiment",
    }

    if table_name not in valid_tables:
        return (
            f"Unknown table '{table_name}'. "
            f"Available: {list(valid_tables.keys())}"
        )

    path = valid_tables[table_name]

    try:
        spark = build_spark_for_query()
        spark.sparkContext.setLogLevel("ERROR")

        df = spark.read.format("delta").load(path)

        # Return recent rows -- limit to avoid overwhelming the LLM context
        if table_name == "price_anomaly_flags":
            # For anomaly flags, prioritize flagged rows first, then recent
            pandas_df = df.orderBy(
                df.is_anomaly.desc(),
                df.bar_time_ts.desc()
            ).limit(20).toPandas()
        else:
            pandas_df = df.orderBy(
                df.published_date.desc()
            ).limit(10).toPandas()

        if pandas_df.empty:
            return f"Table '{table_name}' exists but contains no data yet."

        return pandas_df.to_string(index=False)

    except Exception as e:
        return f"Error reading {table_name}: {str(e)}"


# ── Tool 3: Live Price Fetch ──────────────────────────────────────────────────

@tool
def get_live_price(ticker: str) -> str:
    """
    Get the current or most recent stock price and basic info for a ticker.
    Use this when the user asks about current price, recent performance,
    market cap, or 52-week range for a specific stock.
    
    Input: stock ticker symbol like 'NVDA', 'AAPL', 'MSFT'
    Output: current price, daily change, volume, and key metrics
    """
    try:
        ticker = ticker.upper().strip()
        stock = yf.Ticker(ticker)

        # fast_info is lighter than .info (no heavy scraping)
        info = stock.fast_info

        # Get last 5 days of daily bars for recent context
        hist = stock.history(period="5d", interval="1d")

        if hist.empty:
            return f"No price data available for {ticker}. Market may be closed."

        latest = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else None

        daily_change = ""
        if prev is not None:
            change = latest["Close"] - prev["Close"]
            change_pct = (change / prev["Close"]) * 100
            direction = "+" if change >= 0 else ""
            daily_change = f"{direction}{change:.2f} ({direction}{change_pct:.2f}%)"

        result = (
            f"Ticker: {ticker}\n"
            f"Latest Close: USD {latest['Close']:.2f}\n"
            f"Daily Change: {daily_change}\n"
            f"Volume: {int(latest['Volume']):,}\n"
            f"High: USD {latest['High']:.2f} | Low: USD {latest['Low']:.2f}\n"
        )

        try:
            result += f"52-Week High: USD {info.year_high:.2f} | Low: USD {info.year_low:.2f}\n"
        except Exception:
            pass

        return result

    except Exception as e:
        return f"Error fetching price for {ticker}: {str(e)}"


# Export all tools as a list for the agent graph
TOOLS = [rag_search, query_gold_table, get_live_price]