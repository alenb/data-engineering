import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.bronze import Bronze


# Define the function to run the Bronze ETL process
def run_bronze_python():
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
        task_id="run_bronze_task", python_callable=run_bronze_python
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="silver_train_patrons_yearly",
        wait_for_completion=False,
    )

    bronze_task >> trigger_silver
