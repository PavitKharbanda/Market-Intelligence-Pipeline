"""
Market Pulse - FastAPI Backend

Exposes the LangGraph agent as a REST API.

Endpoints:
  GET  /health       -- health check for Cloud Run
  POST /query        -- run the agent with a user query
  GET  /index-stats  -- how many articles are in the FAISS index

The agent is loaded once at startup (not per request) so the
FAISS index and Spark session are reused across calls.
"""

import os
import time
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from pathlib import Path
import pickle
from agent.tools import _faiss_index

INDEX_PATH = Path("agent/faiss_index/news.index")
METADATA_PATH = Path("agent/faiss_index/metadata.pkl")

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ── Request/Response models ───────────────────────────────────────────────────

class QueryRequest(BaseModel):
    query: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "What's driving NVDA this week and are there any volume anomalies?"
            }
        }


class QueryResponse(BaseModel):
    answer: str
    latency_seconds: float
    query: str


class HealthResponse(BaseModel):
    status: str
    faiss_index_loaded: bool
    articles_indexed: int


# ── Lifespan: runs on startup and shutdown ────────────────────────────────────
# We load the agent once at startup so the FAISS index is in memory
# for all subsequent requests. Without this, every request would
# reload the index from disk (~100ms overhead per call).

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Market Pulse API...")
    # Pre-load the FAISS index into memory at startup
    try:
        from agent.tools import get_faiss_index
        index, metadata = get_faiss_index()
        log.info(f"FAISS index loaded: {index.ntotal} articles")
    except FileNotFoundError:
        log.warning("FAISS index not found -- run agent/embedder.py first")
    yield
    log.info("Shutting down Market Pulse API...")


app = FastAPI(
    title="Market Pulse API",
    description="Real-time market intelligence powered by LangGraph + RAG",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow Streamlit (running on a different port) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    # Just check if the index file exists on disk
    # Don't load it -- that's expensive
    index_exists = INDEX_PATH.exists()
    articles = 0
    
    if index_exists and _faiss_index is not None:
        # Already loaded in memory from startup
        articles = _faiss_index.ntotal
    elif index_exists:
        # File exists but not loaded yet -- count from metadata
        try:
            with open(METADATA_PATH, "rb") as f:
                meta = pickle.load(f)
            articles = len(meta)
        except Exception:
            pass

    return HealthResponse(
        status="healthy" if index_exists else "degraded",
        faiss_index_loaded=index_exists,
        articles_indexed=articles,
    )


@app.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Main endpoint -- runs the LangGraph agent with the user query.
    
    The agent will:
    1. Search news via FAISS RAG
    2. Query gold tables if relevant
    3. Fetch live prices if relevant
    4. Synthesize a grounded answer
    """
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    log.info(f"Received query: {request.query}")
    start = time.time()

    try:
        from agent.graph import run_agent
        answer = run_agent(request.query)
        latency = round(time.time() - start, 2)
        log.info(f"Query completed in {latency}s")
        return QueryResponse(
            answer=answer,
            latency_seconds=latency,
            query=request.query,
        )
    except Exception as e:
        log.error(f"Agent error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/index-stats")
def index_stats():
    """Returns info about the current FAISS index."""
    try:
        from agent.tools import get_faiss_index, _metadata
        index, metadata = get_faiss_index()
        return {
            "articles_indexed": index.ntotal,
            "embedding_dim": index.d,
            "sample_titles": [m["title"] for m in metadata[:3]],
        }
    except Exception as e:
        return {"error": str(e)}