"""
Example DAG demonstrating cron expression reconciliation with the waiter plugin.

This DAG shows how the waiter plugin automatically handles cron expressions.
It demonstrates waiting for demo_hourly_dag which runs hourly.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Import the waiter plugin
from waiter import wait_for_task, dags

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Example: DAG that runs on cron schedule, waiting for a timedelta DAG
# This demonstrates cron-to-timedelta reconciliation
with DAG(
    'example_cron_wait_dag',
    default_args=default_args,
    description='Cron DAG (daily at 3 AM) waiting for hourly timedelta DAG',
    schedule='0 3 * * *',  # Daily at 3 AM (cron expression)
    catchup=False,
) as dag:

    # Wait for demo_hourly_dag which runs hourly (timedelta)
    # Plugin automatically reconciles: cron (daily at 3 AM) waiting for timedelta (hourly)
    # It will wait for the most recent hourly run before 3 AM
    wait_for_hourly = wait_for_task(
        task=dags.demo_hourly_dag.hourly_task,  # References demo_hourly_dag.hourly_task
        task_id='wait_for_hourly',
        current_dag=dag,  # Provide DAG context for cron reconciliation
        mode='reschedule',  # Free worker slot between checks
        poke_interval=300,  # Check every 5 minutes
    )

    # Task that runs after waiting
    process_after_hourly = BashOperator(
        task_id='process_after_hourly',
        bash_command='echo "Processed after waiting for demo_hourly_dag.hourly_task"',
    )

    wait_for_hourly >> process_after_hourly

# Example: DAG that runs hourly, demonstrating schedule reconciliation
# This shows how a more frequent DAG waits for a less frequent one
with DAG(
    'example_hourly_waiting_daily',
    default_args=default_args,
    description='Hourly DAG waiting for daily DAG - demonstrates schedule reconciliation',
    schedule=timedelta(hours=1),  # Runs hourly
    catchup=False,
) as hourly_dag:

    # Wait for simple_dag which runs daily
    # Plugin automatically reconciles: hourly waiting for daily
    # It will wait for the daily run that contains the current hour
    wait_for_daily = wait_for_task(
        task=dags.simple_dag.print_date,  # References simple_dag.print_date
        task_id='wait_for_daily',
        current_dag=hourly_dag,
        mode='reschedule',
        poke_interval=600,  # Check every 10 minutes
    )

    process_after_daily = BashOperator(
        task_id='process_after_daily',
        bash_command='echo "Hourly task processed after waiting for daily simple_dag"',
    )

    wait_for_daily >> process_after_daily

