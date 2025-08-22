"""
Gold Layer DAG - Train Patronage Data Pipeline

This DAG orchestrates the Gold layer of the train patronage data pipeline,
creating dimensional star schema tables and specialised curated analysis tables.
Processes both Gold and Gold Curated layers sequentially.

Schedule: Yearly (@yearly)
Tags: gold, yearly, train patrons
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.gold import Gold
from scripts.gold_curated import GoldCurated


def run_gold() -> None:
    """Execute the Gold layer ETL process for creating star schema dimensions."""
    gold = Gold()
    gold.run()


def run_gold_curated() -> None:
    """Execute the Gold Curated layer ETL process for specialised analysis tables."""
    curated = GoldCurated()
    curated.run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gold_train_patrons_yearly",
    default_args=default_args,
    description="Run Gold ETL and then GoldCurated ETL",
    schedule_interval="@yearly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold", "yearly", "train patrons"],
) as dag:
    
    gold_task = PythonOperator(
        task_id="run_gold_task",
        python_callable=run_gold
    )

    gold_curated_task = PythonOperator(
        task_id="run_gold_curated_task",
        python_callable=run_gold_curated
    )

    gold_task >> gold_curated_task
