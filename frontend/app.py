"""
Market Pulse - Streamlit Frontend

Chat interface for the Market Pulse agent.
Sends queries to the FastAPI backend and displays responses.

Keeps conversation history in session state so the UI
feels like a real chat app.
"""

import time
import streamlit as st
import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:8080")

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Market Pulse",
    page_icon="📈",
    layout="centered",
)

st.title("📈 Market Pulse")
st.caption("Real-time market intelligence powered by AI")

# ── Session state: conversation history ──────────────────────────────────────
# Streamlit reruns the entire script on every interaction.
# st.session_state persists data across reruns.

if "messages" not in st.session_state:
    st.session_state.messages = []

if "index_stats" not in st.session_state:
    # Fetch index stats once on load
    try:
        resp = requests.get(f"{API_URL}/index-stats", timeout=5)
        st.session_state.index_stats = resp.json()
    except Exception:
        st.session_state.index_stats = None

# ── Sidebar: pipeline status ──────────────────────────────────────────────────

with st.sidebar:
    st.header("Pipeline Status")

    # Health check
    try:
        health = requests.get(f"{API_URL}/health", timeout=5).json()
        if health["status"] == "healthy":
            st.success("API: Healthy")
        else:
            st.warning("API: Degraded")
        st.metric("Articles Indexed", health.get("articles_indexed", 0))
    except Exception:
        st.error("API: Unreachable")

    st.divider()
    st.header("Example Queries")
    examples = [
        "What's the latest news in financial markets?",
        "Are there any volume anomalies in recent trading?",
        "What is the current price of NVDA?",
        "What's driving the S&P 500 this week?",
        "Any unusual trading activity today?",
    ]
    for example in examples:
        if st.button(example, use_container_width=True):
            st.session_state.pending_query = example

    st.divider()
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ── Chat history display ──────────────────────────────────────────────────────

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "latency" in message:
            st.caption(f"Response time: {message['latency']}s")


# ── Handle example button clicks ──────────────────────────────────────────────

if "pending_query" in st.session_state:
    query = st.session_state.pop("pending_query")
    st.session_state.messages.append({"role": "user", "content": query})

    with st.chat_message("user"):
        st.markdown(query)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                resp = requests.post(
                    f"{API_URL}/query",
                    json={"query": query},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    answer = data["answer"]
                    latency = data["latency_seconds"]
                else:
                    answer = f"API error {resp.status_code}: {resp.json().get('detail', 'Unknown error')}"
                    latency = None
            except Exception as e:
                answer = f"Error connecting to API: {e}"
                latency = None

        st.markdown(answer)
        if latency:
            st.caption(f"Response time: {latency}s")

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "latency": latency,
    })
    st.rerun()


# ── Chat input ────────────────────────────────────────────────────────────────

if query := st.chat_input("Ask about markets, stocks, or news..."):
    st.session_state.messages.append({"role": "user", "content": query})

    with st.chat_message("user"):
        st.markdown(query)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                resp = requests.post(
                    f"{API_URL}/query",
                    json={"query": query},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    answer = data["answer"]
                    latency = data["latency_seconds"]
                else:
                    answer = f"API error {resp.status_code}: {resp.json().get('detail', 'Unknown error')}"
                    latency = None
            except Exception as e:
                answer = f"Error connecting to API: {e}"
                latency = None

        st.markdown(answer)
        if latency:
            st.caption(f"Response time: {latency}s")

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "latency": latency,
    })
    