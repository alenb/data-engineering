from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.gold import Gold
from scripts.gold_curated import GoldCurated

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def run_gold():
    gold = Gold()
    gold.run()

def run_gold_curated():
    curated = GoldCurated()
    curated.run()

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

    # Set dependency: Gold runs first, then GoldCurated
    gold_task >> gold_curated_task
