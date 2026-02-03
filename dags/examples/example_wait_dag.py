"""
Example DAG demonstrating the waiter plugin.

This DAG shows how to wait for tasks from other DAGs using the waiter plugin.
It waits for tasks from 'simple_dag' which must exist and run first.

To see this work:
1. Ensure 'simple_dag' is enabled and running
2. Trigger 'simple_dag' to run at least once
3. Then trigger this DAG - it will wait for simple_dag's tasks to complete
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

with DAG(
    'example_wait_dag',
    default_args=default_args,
    description='Example DAG using waiter plugin - waits for simple_dag tasks',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    # Example 1: Wait for a task from another DAG using the dags helper
    # This demonstrates: dags.my_dag.my_task_id syntax
    # Schedule reconciliation: Both DAGs run daily, so waits for same execution date
    # Mode: 'poke' keeps worker slot occupied (good for short waits)
    wait_for_simple_dag = wait_for_task(
        task=dags.simple_dag.print_date,  # References simple_dag.print_date task
        task_id='wait_for_simple_dag_print_date',
        current_dag=dag,  # Provide DAG context for automatic schedule reconciliation
        mode='poke',  # Keep worker slot occupied (default)
        retries=2,  # Retry up to 2 times if sensor fails
        retry_delay=timedelta(minutes=5),
        poke_interval=60,  # Check every minute
    )

    # Example 2: Wait for a task using string format with reschedule mode
    # This demonstrates: string format 'dag_id.task_id'
    # Mode: 'reschedule' frees worker slot between checks (better for long waits)
    wait_for_hello = wait_for_task(
        task='simple_dag.print_hello',  # String format reference
        task_id='wait_for_simple_dag_hello',
        current_dag=dag,
        mode='reschedule',  # Free worker slot between checks
        poke_interval=300,  # Check every 5 minutes
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Example 3: Your own task that runs after waiting
    # This task will only run after both wait tasks succeed
    my_task = BashOperator(
        task_id='my_task',
        bash_command='echo "This task runs after waiting for simple_dag.print_date and simple_dag.print_hello to complete"',
    )

    # Set up the dependencies
    # Both wait tasks must complete before my_task runs
    [wait_for_simple_dag, wait_for_hello] >> my_task

