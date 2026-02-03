"""
DAG Template

Copy this template to create a new DAG. Replace all placeholder values.

DAG ID Format: {tenant}_{dag_name}
Example: data-engineering_daily_etl
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'YOUR_TENANT',  # Replace with your team/tenant name
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Set appropriate start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Set appropriate retry count
    'retry_delay': timedelta(minutes=5),  # Set appropriate retry delay
}

with DAG(
    'YOUR_TENANT_YOUR_DAG_NAME',  # Format: {tenant}_{dag_name}
    default_args=default_args,
    description='Description of what this DAG does',
    schedule=timedelta(days=1),  # Set schedule (can be None, timedelta, cron, etc.)
    catchup=False,  # Explicitly set catchup
    max_active_runs=1,  # Limit concurrent runs
    max_active_tasks=3,  # Limit concurrent tasks
    tags=['YOUR_TENANT', 'tag1', 'tag2'],  # Add relevant tags
    doc_md=None,  # Add markdown documentation here if needed
) as dag:

    # Example task
    example_task = BashOperator(
        task_id='example_task',
        bash_command='echo "Hello from YOUR_TENANT_YOUR_DAG_NAME"',
    )

    # Add your tasks here
    # task1 >> task2 >> task3

