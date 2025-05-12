from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_token_path():
    print("MLFLOW_TRACKING_TOKEN_FILE:", os.getenv("MLFLOW_TRACKING_TOKEN_FILE"))
    with open(os.getenv("MLFLOW_TRACKING_TOKEN_FILE"), "r") as f:
        token = f.read().strip()
    print("Token (shortened):", token[:30], "...")

with DAG("debug_mlflow_token", start_date=datetime(2023,1,1), schedule_interval=None, catchup=False) as dag:
    t = PythonOperator(
        task_id="check_token",
        python_callable=print_token_path
    )