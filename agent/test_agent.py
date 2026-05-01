"""
Quick test to verify the agent works before wrapping in FastAPI.
Run this from the project root:
  python -m agent.test_agent
"""

from agent.graph import run_agent
import time

queries = [
    "What's the latest news in financial markets?",
    "Are there any volume anomalies in recent trading?",
    "What is the current price of AAPL?",
]

for query in queries:
    print(f"\n{'='*60}")
    print(f"QUERY: {query}")
    print('='*60)
    start = time.time()
    answer = run_agent(query)
    elapsed = time.time() - start
    print(f"ANSWER ({elapsed:.1f}s):\n{answer}")