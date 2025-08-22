"""
Silver Layer DAG - Train Patronage Data Pipeline

This DAG orchestrates the Silver layer of the train patronage data pipeline,
handling data cleaning and transformation after Bronze layer completion.
Triggers the Gold layer DAG upon successful completion.

Schedule: Yearly (@yearly)
Tags: silver, yearly, train patrons
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.silver import Silver


def run_silver() -> None:
    """Execute the Silver layer ETL process for data cleaning and transformation."""
    silver = Silver()
    silver.run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_train_patrons_yearly",
    default_args=default_args,
    description="Run Silver ETL after Bronze ETL completes",
    schedule_interval="@yearly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "yearly", "train patrons"],
) as dag:

    silver_task = PythonOperator(
        task_id="run_silver_task", 
        python_callable=run_silver
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="gold_train_patrons_yearly",
        wait_for_completion=False,
    )

    silver_task >> trigger_gold
