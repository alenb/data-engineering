"""
Bronze Layer DAG - Train Patronage Data Pipeline

This DAG orchestrates the Bronze layer of the train patronage data pipeline,
handling raw data ingestion from the DataVic API on a yearly schedule.
Triggers the Silver layer DAG upon successful completion.

Schedule: Yearly (@yearly)
Tags: bronze, yearly, train patrons
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.bronze import Bronze


def run_bronze_python() -> None:
    """Execute the Bronze layer ETL process for train patronage data."""
    bronze = Bronze()
    bronze.run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="bronze_train_patrons_yearly",
    default_args=default_args,
    description="Run Bronze ETL once a year",
    schedule_interval="@yearly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "yearly", "train patrons"],
) as dag:

    bronze_task = PythonOperator(
        task_id="run_bronze_task", 
        python_callable=run_bronze_python
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="silver_train_patrons_yearly",
        wait_for_completion=False,
    )

    bronze_task >> trigger_silver
