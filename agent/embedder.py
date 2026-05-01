"""
Market Pulse - News Embedder
Reads silver/news Delta table, embeds articles using OpenAI,
saves FAISS index to disk at agent/faiss_index/

Run this:
  - Once to build the initial index
  - On a schedule (Airflow) to refresh as new articles arrive

The index persists on disk so the agent can load it instantly
without re-embedding everything on every startup.
"""

import os
import json
import pickle
from pathlib import Path

from openai import OpenAI
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import faiss
import numpy as np
from dotenv import load_dotenv

load_dotenv()

DELTA_ROOT = os.getenv("DELTA_ROOT", "./delta_lake")
INDEX_DIR = Path("agent/faiss_index")
INDEX_DIR.mkdir(parents=True, exist_ok=True)

INDEX_PATH = INDEX_DIR / "news.index"       # FAISS binary index
METADATA_PATH = INDEX_DIR / "metadata.pkl"  # article metadata parallel to index

EMBEDDING_MODEL = "text-embedding-3-small"  # 1536 dims, cheap, fast
EMBEDDING_DIM = 1536
BATCH_SIZE = 50   # OpenAI allows up to 2048 inputs per call, 50 is safe


def build_spark():
    return (
        SparkSession.builder
        .appName("MarketPulse-Embedder")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def article_to_text(row) -> str:
    """
    Combine article fields into a single string for embedding.
    
    Why combine fields? Each field adds signal:
    - title: most information-dense, always present
    - description: adds context, usually 1-2 sentences
    - content: truncated to 200 chars by NewsAPI free tier, still useful
    
    We concatenate with separators so the model sees them as
    distinct sections, not one run-on string.
    """
    parts = []
    if row.title:
        parts.append(f"Title: {row.title}")
    if row.description:
        parts.append(f"Description: {row.description}")
    if row.content:
        # NewsAPI truncates content at ~200 chars with "[+N chars]" suffix
        # Strip that suffix before embedding
        content = row.content.split("[+")[0].strip()
        if content:
            parts.append(f"Content: {content}")
    return " | ".join(parts)


def get_embeddings(client: OpenAI, texts: list[str]) -> list[list[float]]:
    """
    Call OpenAI embeddings API for a batch of texts.
    
    Returns a list of vectors, one per input text.
    Each vector is a list of 1536 floats.
    
    Cost note: text-embedding-3-small costs $0.02 per 1M tokens.
    A typical news article title+description is ~100 tokens.
    100 articles = 10,000 tokens = $0.0002. Basically free.
    """
    response = client.embeddings.create(
        input=texts,
        model=EMBEDDING_MODEL,
    )
    # Response contains embeddings in the same order as input
    return [item.embedding for item in response.data]


def build_index():
    print("Starting embedder...")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # ── Load silver news from Delta Lake ─────────────────────────────────────
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_path = f"{DELTA_ROOT}/silver/news"
    print(f"Reading silver news from {silver_path}...")

    df = spark.read.format("delta").load(silver_path)
    rows = df.select("title", "description", "content", "url", "source_name", "published_at").collect()
    spark.stop()

    print(f"Loaded {len(rows)} articles from silver")

    if not rows:
        print("No articles found. Run silver transform first.")
        return

    # ── Build text strings for embedding ─────────────────────────────────────
    texts = []
    metadata = []   # parallel list -- metadata[i] corresponds to index vector i

    for row in rows:
        text = article_to_text(row)
        if not text.strip():
            continue
        texts.append(text)
        metadata.append({
            "title": row.title,
            "url": row.url,
            "source_name": row.source_name,
            "published_at": row.published_at,
            "text": text,   # store full text so we can return it to the agent
        })

    print(f"Embedding {len(texts)} articles in batches of {BATCH_SIZE}...")

    # ── Embed in batches ──────────────────────────────────────────────────────
    all_vectors = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        print(f"  Batch {i//BATCH_SIZE + 1}: embedding {len(batch)} articles...")
        vectors = get_embeddings(client, batch)
        all_vectors.extend(vectors)

    print(f"Generated {len(all_vectors)} embeddings")

    # ── Build FAISS index ─────────────────────────────────────────────────────
    # IndexFlatIP = Inner Product index
    # We use inner product (dot product) because OpenAI vectors are normalized --
    # dot product of two normalized vectors = cosine similarity.
    # Higher score = more similar meaning.
    #
    # IndexFlatIP is exact (no approximation) -- fine for hundreds of articles.
    # For millions of articles you'd use IndexIVFFlat (approximate but faster).
    vectors_np = np.array(all_vectors, dtype=np.float32)

    # Normalize vectors for cosine similarity via inner product
    faiss.normalize_L2(vectors_np)

    index = faiss.IndexFlatIP(EMBEDDING_DIM)
    index.add(vectors_np)

    print(f"FAISS index built with {index.ntotal} vectors")

    # ── Save index and metadata to disk ──────────────────────────────────────
    faiss.write_index(index, str(INDEX_PATH))

    with open(METADATA_PATH, "wb") as f:
        pickle.dump(metadata, f)

    print(f"Saved index to {INDEX_PATH}")
    print(f"Saved metadata to {METADATA_PATH}")
    print("Embedder complete.")


if __name__ == "__main__":
    build_index()