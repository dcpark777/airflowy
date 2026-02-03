"""Custom operators for waiting on external tasks."""

from datetime import datetime, timedelta
from typing import Optional

from airflow.models import TaskInstance
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState


class WaitForTaskOperator(ExternalTaskSensor):
    """
    Operator that waits for a task in another DAG to complete.

    This is a convenience wrapper around ExternalTaskSensor that makes it
    easier to wait for specific tasks in other DAGs.

    Args:
        external_dag_id: The DAG ID of the external DAG
        external_task_id: The task ID to wait for in the external DAG
        execution_delta: Time difference with the previous execution to look at
        execution_date_fn: Function that receives the current execution date
            and returns the desired execution dates to query
        allowed_states: List of allowed states, default is ['success']
        failed_states: List of failed or dis-allowed states, default is None
        check_existence: Whether to check if the external task exists
        poke_interval: Time in seconds that the job should wait in between
            each try
        timeout: Time in seconds before the task times out and fails
        soft_fail: Set to true to mark the task as SKIPPED on failure
        mode: How the sensor operates. Options:
            - 'poke' (default): Sensor runs continuously, checking at intervals
            - 'reschedule': Sensor reschedules itself between checks, freeing worker slot
        retries: Number of times to retry the sensor if it fails
        retry_delay: Time to wait between retries
        **kwargs: Additional arguments passed to BaseSensorOperator

    Example:
        ```python
        from airflow_waiter import WaitForTaskOperator

        wait_task = WaitForTaskOperator(
            task_id='wait_for_other_dag',
            external_dag_id='my_dag',
            external_task_id='my_task',
        )
        ```
    """

    def __init__(
        self,
        external_dag_id: str,
        external_task_id: str,
        execution_delta: Optional[timedelta] = None,
        execution_date_fn: Optional[callable] = None,
        allowed_states: Optional[list] = None,
        failed_states: Optional[list] = None,
        check_existence: bool = True,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,  # 7 days default
        soft_fail: bool = False,
        mode: str = 'poke',
        retries: int = 0,
        retry_delay: timedelta = timedelta(minutes=5),
        **kwargs,
    ):
        if allowed_states is None:
            allowed_states = [TaskInstanceState.SUCCESS]

        # Set retries and retry_delay in kwargs if not already set
        if 'retries' not in kwargs:
            kwargs['retries'] = retries
        if 'retry_delay' not in kwargs:
            kwargs['retry_delay'] = retry_delay

        super().__init__(
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_delta=execution_delta,
            execution_date_fn=execution_date_fn,
            allowed_states=allowed_states,
            failed_states=failed_states,
            check_existence=check_existence,
            poke_interval=poke_interval,
            timeout=timeout,
            soft_fail=soft_fail,
            mode=mode,
            **kwargs,
        )

