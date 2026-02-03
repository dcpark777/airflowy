"""
Demo DAG that runs hourly - used to demonstrate schedule reconciliation.

This DAG runs every hour and can be waited on by other DAGs to demonstrate
how the waiter plugin handles different schedule intervals.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'demo_hourly_dag',
    default_args=default_args,
    description='Demo DAG that runs hourly - for waiter plugin demonstrations',
    schedule=timedelta(hours=1),  # Runs every hour
    catchup=False,
) as dag:

    hourly_task = BashOperator(
        task_id='hourly_task',
        bash_command='echo "Hourly task completed at $(date)"',
    )

