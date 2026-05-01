"""
Market Pulse - LangGraph Agent

Architecture:
  User message
      ↓
  LLM node (GPT-4o-mini with tools bound)
      ↓ (if tool_calls in response)
  Tool node (executes the called tool)
      ↓
  LLM node again (with tool results in context)
      ↓ (if no more tool_calls)
  Final answer returned

This is the ReAct pattern (Reasoning + Acting):
  Reason: LLM decides what to do
  Act: call a tool
  Observe: get tool result
  Repeat until done

LangGraph implements this as a graph with two nodes (llm, tools)
and conditional edges (if LLM called a tool → go to tools node,
otherwise → end).
"""

import os
from typing import Annotated
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from typing_extensions import TypedDict

from agent.tools import TOOLS

load_dotenv()

# ── State definition ──────────────────────────────────────────────────────────
# LangGraph passes a State object between nodes.
# The state holds the full conversation history as a list of messages.
# add_messages is a reducer -- it appends new messages instead of replacing.

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


# ── LLM setup ─────────────────────────────────────────────────────────────────
# GPT-4o-mini: cheap, fast, good at tool calling. 
# .bind_tools() tells the LLM what tools are available and their schemas.
# The LLM can then respond with tool_call objects instead of text.

llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,          # 0 = deterministic, no creativity -- we want factual answers
    api_key=os.getenv("OPENAI_API_KEY"),
)

llm_with_tools = llm.bind_tools(TOOLS)

SYSTEM_PROMPT = """You are Market Pulse, a financial market intelligence assistant.
You have access to real-time news, market data, and anomaly detection tools.

When answering questions:
1. Always search for relevant news using rag_search first
2. Check price anomaly flags if the user asks about unusual activity
3. Get live prices when asked about current stock performance
4. Ground your answers in the data returned by tools -- do not speculate
5. Be concise but specific -- cite article titles and data points
6. When reporting prices, always use the format "USD X.XX" -- never use the dollar sign symbol

If data is unavailable (e.g. market closed, no articles yet), say so clearly.
"""


# ── Graph nodes ───────────────────────────────────────────────────────────────

def llm_node(state: AgentState) -> AgentState:
    """
    The LLM node: takes current message history, calls the LLM,
    appends the response to the state.
    
    If the LLM decides to call a tool, its response will contain
    tool_calls -- the graph routes to the tool node.
    If no tool calls, the graph ends and returns the final answer.
    """
    messages = state["messages"]

    # Prepend system prompt on first call
    if not any(isinstance(m, SystemMessage) for m in messages):
        messages = [SystemMessage(content=SYSTEM_PROMPT)] + messages

    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    """
    Conditional edge: decides where to go after the LLM node.
    
    If the last message has tool_calls → go to "tools" node
    If no tool_calls → go to END (return final answer)
    """
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    return END


# ── Build the graph ───────────────────────────────────────────────────────────

def build_graph():
    """
    Constructs the LangGraph state machine.
    
    Graph structure:
      START → llm_node
      llm_node → (if tool_calls) → tool_node → llm_node
      llm_node → (if no tool_calls) → END
    """
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("llm", llm_node)
    graph.add_node("tools", ToolNode(TOOLS))

    # Add edges
    graph.set_entry_point("llm")
    graph.add_conditional_edges("llm", should_continue)

    # After tools run, always go back to LLM
    # (LLM needs to see tool results and decide what to do next)
    graph.add_edge("tools", "llm")

    return graph.compile()


# Compiled graph -- import this in FastAPI and tests
agent = build_graph()


def run_agent(query: str) -> str:
    """
    Run the agent with a user query and return the final text answer.
    This is the main entry point used by FastAPI.
    """
    result = agent.invoke({
        "messages": [HumanMessage(content=query)]
    })

    # Last message is the final LLM response
    final_message = result["messages"][-1]
    return final_message.content