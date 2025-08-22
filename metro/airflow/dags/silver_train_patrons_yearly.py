from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.silver import Silver

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_silver():
    silver = Silver()
    silver.run()


with DAG(
    dag_id="silver_train_patrons_yearly",
    default_args=default_args,
    description="Run Silver ETL after Bronze ETL completes",
    schedule_interval="@yearly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "yearly", "train patrons"],
) as dag:

    silver_task = PythonOperator(task_id="run_silver_task", python_callable=run_silver)

    # Trigger the Gold DAG after Silver completes
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="gold_train_patrons_yearly",
        wait_for_completion=False,
    )

    # Set dependency: Silver runs first, then trigger Gold
    silver_task >> trigger_gold
