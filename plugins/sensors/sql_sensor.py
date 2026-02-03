"""
Sensor that waits for a SQL query to return a result.
Use cases:
1. Wait for a SQL query to return a result.
2. Option to save/persist the result of the SQL query to a variable.

Should have retries, poke_interval, timeout, and other arguments that are common to all sensors.
"""

from datetime import timedelta
from typing import Any, Optional, Union

from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class SqlSensor(BaseSensorOperator):
    """
    Sensor that waits for a SQL query to return a result.

    This sensor executes a SQL query repeatedly until it returns a result
    (non-empty result set). It can optionally save the query result to XCom
    for use by downstream tasks.

    Args:
        sql: The SQL query to execute. Can be a string or a callable that
            returns a string. The callable will receive the context as an argument.
        conn_id: The connection ID to use for the database connection.
            Defaults to None, which will use the default connection.
        parameters: Optional parameters to pass to the SQL query (for parameterized queries).
            Can be a dict, list, or tuple.
        save_result: If True, save the query result to XCom. Defaults to False.
        result_key: The key to use when saving the result to XCom. Defaults to 'sql_result'.
        poke_interval: Time in seconds that the job should wait in between each try.
            Defaults to 60 seconds.
        timeout: Time in seconds before the task times out and fails.
            Defaults to 7 days (604800 seconds).
        soft_fail: Set to True to mark the task as SKIPPED on failure instead of FAILED.
            Defaults to False.
        mode: How the sensor operates. Options:
            - 'poke' (default): Sensor runs continuously, checking at intervals.
              Keeps the worker slot occupied.
            - 'reschedule': Sensor reschedules itself between checks, freeing worker slot.
              Better for long waits with infrequent checks.
        retries: Number of times to retry the sensor if it fails. Defaults to 0.
        retry_delay: Time to wait between retries. Defaults to 5 minutes.
        **kwargs: Additional arguments passed to BaseSensorOperator.

    Example:
        ```python
        from airflow import DAG
        from sensors import SqlSensor
        from datetime import datetime

        with DAG('my_dag', start_date=datetime(2023, 1, 1)) as dag:
            # Wait for a query to return results
            wait_for_data = SqlSensor(
                task_id='wait_for_data',
                sql='SELECT COUNT(*) FROM my_table WHERE status = %s',
                conn_id='my_db_connection',
                parameters=['ready'],
                poke_interval=60,
                timeout=3600,
            )

            # Wait and save result to XCom
            wait_and_save = SqlSensor(
                task_id='wait_and_save',
                sql='SELECT value FROM config_table WHERE key = %s',
                conn_id='my_db_connection',
                parameters=['config_value'],
                save_result=True,
                result_key='config_value',
            )
        ```
    """

    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)

    def __init__(
        self,
        sql: Union[str, callable],
        conn_id: Optional[str] = None,
        parameters: Optional[Union[dict, list, tuple]] = None,
        save_result: bool = False,
        result_key: str = 'sql_result',
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,  # 7 days default
        soft_fail: bool = False,
        mode: str = 'poke',
        retries: int = 0,
        retry_delay: timedelta = timedelta(minutes=5),
        **kwargs,
    ):
        # Set retries and retry_delay in kwargs if not already set
        if 'retries' not in kwargs:
            kwargs['retries'] = retries
        if 'retry_delay' not in kwargs:
            kwargs['retry_delay'] = retry_delay

        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            mode=mode,
            **kwargs,
        )
        self.sql = sql
        self.conn_id = conn_id
        self.parameters = parameters
        self.save_result = save_result
        self.result_key = result_key

    def poke(self, context: Context) -> bool:
        """
        Execute the SQL query and check if it returns a result.

        Args:
            context: The task execution context.

        Returns:
            True if the query returns a result, False otherwise.
        """
        # Get the SQL query (handle both string and callable)
        if callable(self.sql):
            sql_query = self.sql(context)
        else:
            sql_query = self.sql

        # Get database connection hook
        hook = BaseHook.get_hook(conn_id=self.conn_id)

        # Execute the query
        try:
            # Try different methods based on hook type
            # Most SQL hooks (from airflow.providers.common.sql) have get_records
            if hasattr(hook, 'get_records'):
                records = hook.get_records(sql=sql_query, parameters=self.parameters)
            # Some hooks have get_first
            elif hasattr(hook, 'get_first'):
                first_record = hook.get_first(sql=sql_query, parameters=self.parameters)
                records = [first_record] if first_record is not None else []
            # Fallback: use run with a handler
            elif hasattr(hook, 'run'):
                def fetch_all_handler(cursor):
                    return cursor.fetchall()
                records = hook.run(sql=sql_query, parameters=self.parameters, handler=fetch_all_handler)
            else:
                # Last resort: try to get connection and execute manually
                conn = hook.get_conn()
                cursor = conn.cursor()
                try:
                    cursor.execute(sql_query, self.parameters)
                    records = cursor.fetchall()
                finally:
                    cursor.close()
                    conn.close()

            # Handle different result formats
            if records is None:
                has_results = False
            elif isinstance(records, list):
                has_results = len(records) > 0
                # Check if it's a list of empty lists/tuples or None values
                if has_results:
                    # Filter out None values and empty rows
                    has_results = any(
                        row is not None and 
                        (not isinstance(row, (list, tuple)) or len(row) > 0) and
                        (not isinstance(row, (list, tuple)) or any(cell is not None for cell in row))
                        for row in records
                    )
            else:
                # Single value result
                has_results = records is not None and records != 0 and records != ''

            # Save result to XCom if requested
            if has_results and self.save_result:
                self._save_result_to_xcom(context, records)

            return has_results

        except Exception as e:
            # Log the error but don't fail immediately
            # The sensor will retry based on retries and retry_delay
            self.log.warning(f"SQL query failed: {e}")
            return False

    def _save_result_to_xcom(self, context: Context, records: Any) -> None:
        """
        Save the query result to XCom.

        Args:
            context: The task execution context.
            records: The query result records.
        """
        # Get the task instance
        ti = context['ti']

        # Save to XCom
        ti.xcom_push(key=self.result_key, value=records)
        self.log.info(f"Saved query result to XCom with key '{self.result_key}'")
