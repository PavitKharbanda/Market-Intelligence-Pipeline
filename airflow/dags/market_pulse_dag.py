"""
Market Pulse - Airflow DAG
Orchestrates the batch pipeline on a daily schedule:

  silver_transform → dbt_gold_sentiment
                   → dbt_gold_anomaly_flags

Both dbt models run in parallel after silver completes.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import subprocess


default_args = {
    "owner": "pavit",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="market_pulse_pipeline",
    description="Silver transform + dbt gold models",
    schedule="0 6 * * 1-5",   # 6am weekdays — runs after US market open data settles
    start_date=datetime(2026, 4, 24),
    catchup=False,              # don't backfill missed runs
    default_args=default_args,
    tags=["market-pulse"],
)
def market_pulse_pipeline():

    # ── Task 1: Run silver PySpark transform ─────────────────────────────────
    # BashOperator runs a shell command as a task.
    # We call docker-compose to run the silver job in its own container.
    # The --rm flag removes the container after it exits.
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            "docker-compose -f /opt/airflow/dags/../../../docker-compose.yml "
            "--profile silver run --rm spark-silver"
        ),
    )

    # ── Task 2a: dbt gold_daily_sentiment ─────────────────────────────────
    dbt_sentiment = BashOperator(
        task_id="dbt_gold_sentiment",
        bash_command=(
            "cd /opt/airflow/dags/../../../dbt/market_pulse && "
            "dbt run --select gold_daily_sentiment --profiles-dir ."
        ),
    )

    # ── Task 2b: dbt gold_price_anomaly_flags ─────────────────────────────
    dbt_anomaly = BashOperator(
        task_id="dbt_gold_anomaly_flags",
        bash_command=(
            "cd /opt/airflow/dags/../../../dbt/market_pulse && "
            "dbt run --select gold_price_anomaly_flags --profiles-dir ."
        ),
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # silver must complete before either dbt model runs
    # the two dbt models are independent of each other → run in parallel
    silver_transform >> [dbt_sentiment, dbt_anomaly]


market_pulse_pipeline()