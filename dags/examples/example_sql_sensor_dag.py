"""
Example DAG demonstrating the SQL sensor plugin.

This DAG shows how to use the SqlSensor to wait for SQL queries to return results.
It demonstrates various use cases including:
1. Basic SQL sensor usage
2. Using SQL parameters
3. Saving results to XCom
4. Using dynamic SQL queries (callable)
5. Different sensor modes (poke vs reschedule)

Note: This DAG requires a database connection configured in Airflow.
You'll need to set up a connection (e.g., 'my_db_connection') with your database credentials.

Example connection setup:
- Connection ID: my_db_connection
- Connection Type: Postgres (or your database type)
- Host: your-db-host
- Schema: your-database-name
- Login: your-username
- Password: your-password
- Port: 5432 (or your port)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Import the SQL sensor
from sensors import SqlSensor

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
    'example_sql_sensor_dag',
    default_args=default_args,
    description='Example DAG using SQL sensor - waits for SQL queries to return results',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    # Example 1: Basic SQL sensor - wait for a query to return results
    # This sensor checks if a table has any rows matching a condition
    # It will keep checking every 60 seconds until the query returns results
    wait_for_data = SqlSensor(
        task_id='wait_for_data_ready',
        sql='SELECT COUNT(*) FROM data_table WHERE status = %s AND created_at >= CURRENT_DATE',
        conn_id='my_db_connection',  # Your database connection ID
        parameters=['ready'],  # SQL parameters for the query
        poke_interval=60,  # Check every minute
        timeout=3600,  # Timeout after 1 hour
        mode='poke',  # Keep worker slot occupied (good for short waits)
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Example 2: SQL sensor with reschedule mode
    # This is better for longer waits - frees the worker slot between checks
    # Waits for a batch job to complete (marked by a flag in the database)
    wait_for_batch_complete = SqlSensor(
        task_id='wait_for_batch_complete',
        sql='SELECT job_id FROM batch_jobs WHERE status = %s AND batch_date = %s',
        conn_id='my_db_connection',
        parameters=['completed', '{{ ds }}'],  # Use Airflow template variable for date
        poke_interval=300,  # Check every 5 minutes
        timeout=7200,  # Timeout after 2 hours
        mode='reschedule',  # Free worker slot between checks (better for long waits)
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # Example 3: SQL sensor that saves result to XCom
    # This allows downstream tasks to use the query result
    wait_and_get_config = SqlSensor(
        task_id='wait_and_get_config',
        sql='SELECT config_value FROM app_config WHERE config_key = %s',
        conn_id='my_db_connection',
        parameters=['max_processing_time'],
        save_result=True,  # Save result to XCom
        result_key='max_processing_time',  # Key to use in XCom
        poke_interval=30,
        timeout=1800,
    )

    # Example 4: SQL sensor with dynamic SQL (callable)
    # The SQL query is generated dynamically based on the execution context
    def generate_sql_query(context):
        """Generate SQL query dynamically based on execution date."""
        execution_date = context['ds']  # Execution date as string (YYYY-MM-DD)
        return f"""
            SELECT COUNT(*) 
            FROM daily_metrics 
            WHERE metric_date = '{execution_date}' 
            AND metric_value > 1000
        """

    wait_for_daily_metrics = SqlSensor(
        task_id='wait_for_daily_metrics',
        sql=generate_sql_query,  # Pass callable instead of string
        conn_id='my_db_connection',
        poke_interval=120,
        timeout=3600,
        mode='poke',
    )

    # Example 5: Using the result from XCom in a downstream task
    # This task retrieves the value saved by wait_and_get_config
    def use_sql_result(**context):
        """Use the SQL result from XCom."""
        # Pull the value from XCom
        max_time = context['ti'].xcom_pull(
            key='max_processing_time',
            task_ids='wait_and_get_config'
        )
        
        # The result is a list of tuples from the SQL query
        if max_time and len(max_time) > 0:
            value = max_time[0][0] if isinstance(max_time[0], tuple) else max_time[0]
            print(f"Retrieved max_processing_time from database: {value}")
            return value
        else:
            print("No value found in XCom")
            return None

    use_config_task = PythonOperator(
        task_id='use_config_value',
        python_callable=use_sql_result,
    )

    # Example 6: Simple task that runs after SQL sensors complete
    process_data = BashOperator(
        task_id='process_data',
        bash_command='echo "Processing data after SQL conditions are met"',
    )

    # Set up task dependencies
    # wait_for_data and wait_for_batch_complete can run in parallel
    # wait_and_get_config runs independently
    # wait_for_daily_metrics runs independently
    # All must complete before process_data runs
    # use_config_task depends on wait_and_get_config
    [wait_for_data, wait_for_batch_complete, wait_for_daily_metrics] >> process_data
    wait_and_get_config >> use_config_task >> process_data

