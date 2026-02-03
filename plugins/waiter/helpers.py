"""Helper functions for waiting on tasks."""

from typing import Optional, Union, Callable, Tuple
from datetime import timedelta, datetime
import re

from airflow import DAG
from airflow.models import BaseOperator, DagBag

from waiter.operators import WaitForTaskOperator

# Try to import croniter for cron parsing, fall back to basic parsing if not available
try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False


class TaskReference:
    """
    Reference to a task in another DAG.

    This allows you to create references to tasks that can be used
    with wait_for_task().

    Example:
        ```python
        from waiter import TaskReference

        my_task_ref = TaskReference(dag_id='my_dag', task_id='my_task')
        wait_task = wait_for_task(task=my_task_ref)
        ```
    """

    def __init__(self, dag_id: str, task_id: str):
        self.dag_id = dag_id
        self.task_id = task_id

    def __repr__(self):
        return f"TaskReference(dag_id='{self.dag_id}', task_id='{self.task_id}')"


def _is_cron_expression(schedule: str) -> bool:
    """Check if a string is a cron expression."""
    if not isinstance(schedule, str):
        return False
    
    # Airflow preset schedules (not cron)
    presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
    if schedule in presets:
        return False
    
    # Basic cron pattern: 5 fields (minute hour day month weekday)
    cron_pattern = r'^(\S+\s+){4}\S+$'
    return bool(re.match(cron_pattern, schedule.strip()))


def _preset_to_timedelta(preset: str) -> Optional[timedelta]:
    """Convert Airflow preset schedule to timedelta."""
    presets = {
        '@once': None,  # Manual
        '@hourly': timedelta(hours=1),
        '@daily': timedelta(days=1),
        '@weekly': timedelta(weeks=1),
        '@monthly': timedelta(days=30),  # Approximate
        '@yearly': timedelta(days=365),  # Approximate
    }
    return presets.get(preset)


def _cron_to_approximate_timedelta(cron_expr: str) -> Optional[timedelta]:
    """
    Convert a cron expression to an approximate timedelta.
    This is used for comparison purposes only.
    """
    if not HAS_CRONITER:
        # Basic parsing without croniter
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            return None
        
        minute, hour, day, month, weekday = parts
        
        # Check for common patterns
        if minute == '0' and hour == '*':
            # Every hour
            return timedelta(hours=1)
        elif minute == '0' and hour != '*' and day == '*' and month == '*' and weekday == '*':
            # Daily at specific hour
            return timedelta(days=1)
        elif minute == '0' and hour == '0' and day == '*' and month == '*' and weekday == '*':
            # Daily at midnight
            return timedelta(days=1)
        elif minute == '0' and hour == '0' and day == '1' and month == '*':
            # Monthly
            return timedelta(days=30)
        elif minute == '0' and hour == '0' and day == '*' and month == '*' and weekday != '*':
            # Weekly on specific day
            return timedelta(weeks=1)
        
        return None
    
    # Use croniter for more accurate parsing
    try:
        # Get next few occurrences to estimate frequency
        base_time = datetime(2024, 1, 1, 0, 0, 0)
        iter_obj = croniter(cron_expr, base_time)
        
        # Get next 3 occurrences
        times = [iter_obj.get_next(datetime) for _ in range(3)]
        
        if len(times) >= 2:
            # Calculate average interval
            intervals = [(times[i+1] - times[i]).total_seconds() for i in range(len(times)-1)]
            avg_interval = sum(intervals) / len(intervals)
            return timedelta(seconds=avg_interval)
    except Exception:
        pass
    
    return None


def _calculate_cron_reconciliation(
    current_schedule,
    external_schedule,
    current_is_cron: bool,
    external_is_cron: bool,
) -> Tuple[Optional[timedelta], Optional[Callable]]:
    """
    Calculate reconciliation when one or both schedules are cron expressions.
    """
    # If both are cron and identical, no delta needed
    if current_is_cron and external_is_cron and current_schedule == external_schedule:
        return timedelta(0), None
    
    # Convert cron to approximate timedelta for comparison
    current_approx = None
    external_approx = None
    
    if current_is_cron:
        current_approx = _cron_to_approximate_timedelta(current_schedule)
    elif isinstance(current_schedule, timedelta):
        current_approx = current_schedule
    elif isinstance(current_schedule, str):
        current_approx = _preset_to_timedelta(current_schedule)
    
    if external_is_cron:
        external_approx = _cron_to_approximate_timedelta(external_schedule)
    elif isinstance(external_schedule, timedelta):
        external_approx = external_schedule
    elif isinstance(external_schedule, str):
        external_approx = _preset_to_timedelta(external_schedule)
    
    # If we can't approximate, use execution_date_fn with croniter
    if external_is_cron and HAS_CRONITER:
        def execution_date_fn(execution_date: datetime) -> datetime:
            """Find the most recent external cron execution before or at execution_date."""
            try:
                # Get the previous occurrence of the external cron schedule
                iter_obj = croniter(external_schedule, execution_date)
                prev_time = iter_obj.get_prev(datetime)
                return prev_time
            except Exception:
                # Fallback: return execution_date
                return execution_date
        
        return None, execution_date_fn
    
    # If we have approximations, use them
    if current_approx is not None and external_approx is not None:
        if current_approx > external_approx:
            # Current runs less frequently, external runs more frequently
            return timedelta(0), None
        elif current_approx < external_approx:
            # Current runs more frequently, need to find external period
            if external_is_cron:
                # Use execution_date_fn to find the cron occurrence
                if HAS_CRONITER:
                    def execution_date_fn(execution_date: datetime) -> datetime:
                        try:
                            iter_obj = croniter(external_schedule, execution_date)
                            return iter_obj.get_prev(datetime)
                        except Exception:
                            return execution_date
                    return None, execution_date_fn
            else:
                # External is timedelta, use existing logic
                def execution_date_fn(execution_date: datetime) -> datetime:
                    if external_approx >= timedelta(days=1):
                        return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    epoch = datetime(1970, 1, 1)
                    total_seconds = (execution_date - epoch).total_seconds()
                    period_seconds = external_approx.total_seconds()
                    periods = int(total_seconds / period_seconds)
                    return epoch + timedelta(seconds=periods * period_seconds)
                return None, execution_date_fn
        else:
            # Same approximate frequency
            return timedelta(0), None
    
    # Can't reconcile
    return None, None


def _calculate_execution_delta(
    current_dag: DAG,
    external_dag_id: str,
) -> Tuple[Optional[timedelta], Optional[Callable]]:
    """
    Calculate the appropriate execution_delta or execution_date_fn based on schedule differences.

    This function compares the schedules of the current DAG and the external DAG
    to determine the correct timing for waiting on the external task.

    Args:
        current_dag: The DAG where wait_for_task is being called
        external_dag_id: The DAG ID of the external DAG to wait for

    Returns:
        Tuple of (execution_delta, execution_date_fn). One will be set, the other None.
    """
    try:
        # Load the external DAG
        # Use a module-level cached DagBag to avoid recreating it
        # This prevents hangs during DAG loading
        if not hasattr(_calculate_execution_delta, '_cached_dagbag'):
            from airflow.configuration import conf
            dag_folder = conf.get('core', 'dags_folder', fallback=None)
            if dag_folder:
                _calculate_execution_delta._cached_dagbag = DagBag(
                    dag_folder=dag_folder,
                    include_examples=False
                )
            else:
                # Try to get dag_folder from current_dag if available
                if hasattr(current_dag, 'folder') and current_dag.folder:
                    _calculate_execution_delta._cached_dagbag = DagBag(
                        dag_folder=current_dag.folder,
                        include_examples=False
                    )
                else:
                    # Last resort: use default (but this might be slow)
                    _calculate_execution_delta._cached_dagbag = DagBag(include_examples=False)
        
        dagbag = _calculate_execution_delta._cached_dagbag
        external_dag = dagbag.get_dag(external_dag_id)
        
        # If DAG not found, don't try to refresh from DB (this can hang)
        # Just skip auto-calculation - user can provide execution_delta manually
        if external_dag is None:
            # DAG not found, can't auto-calculate
            return None, None

        # Support both 'schedule' (Airflow 3.0) and 'schedule_interval' (Airflow 2.x) attributes
        def get_schedule(dag: DAG):
            """Get schedule from DAG, supporting both Airflow 2.x and 3.x."""
            return getattr(dag, 'schedule', getattr(dag, 'schedule_interval', None))
        
        current_schedule = get_schedule(current_dag)
        external_schedule = get_schedule(external_dag)

        # If schedules are the same, no delta needed
        if current_schedule == external_schedule:
            return timedelta(0), None

        # Handle None schedules (manual/triggered DAGs)
        if current_schedule is None or external_schedule is None:
            return None, None

        # Check if either schedule is a cron expression
        current_is_cron = isinstance(current_schedule, str) and _is_cron_expression(current_schedule)
        external_is_cron = isinstance(external_schedule, str) and _is_cron_expression(external_schedule)
        
        # Handle cron expressions
        if current_is_cron or external_is_cron:
            return _calculate_cron_reconciliation(
                current_schedule, external_schedule, current_is_cron, external_is_cron
            )
        
        # Convert schedules to timedelta for comparison
        def schedule_to_timedelta(schedule):
            """Convert schedule interval to timedelta."""
            if isinstance(schedule, timedelta):
                return schedule
            elif isinstance(schedule, str):
                # Not a cron, might be a preset like '@daily'
                return _preset_to_timedelta(schedule)
            elif hasattr(schedule, 'delta'):
                # Timetable objects
                return schedule.delta
            return None

        current_delta = schedule_to_timedelta(current_schedule)
        external_delta = schedule_to_timedelta(external_schedule)

        if current_delta is None or external_delta is None:
            # Can't auto-calculate for complex schedules
            return None, None

        # Calculate the delta or execution_date_fn
        # The goal is to find the external DAG run that should have completed
        # before or at the current DAG's execution date
        
        if current_delta > external_delta:
            # Current DAG runs less frequently (e.g., daily) waiting for
            # external DAG that runs more frequently (e.g., hourly)
            # We want the most recent external run at or before current execution date
            # Same execution date works because external runs more frequently
            return timedelta(0), None
        elif current_delta < external_delta:
            # Current DAG runs more frequently (e.g., hourly) waiting for
            # external DAG that runs less frequently (e.g., daily)
            # We need to find the external run that contains this execution time
            # For daily external, hourly current: daily run at 00:00 covers all hours that day
            # Use execution_date_fn to round down to the start of the external period
            def execution_date_fn(execution_date: datetime) -> datetime:
                """Round down to the start of the external DAG's period."""
                # For daily schedules, round down to start of day
                if external_delta >= timedelta(days=1):
                    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
                # For other intervals, calculate the start of the period
                # Find how many external_delta periods have passed since epoch
                epoch = datetime(1970, 1, 1)
                total_seconds = (execution_date - epoch).total_seconds()
                period_seconds = external_delta.total_seconds()
                periods = int(total_seconds / period_seconds)
                return epoch + timedelta(seconds=periods * period_seconds)
            
            return None, execution_date_fn
        else:
            # Same frequency - same execution date
            return timedelta(0), None

    except Exception:
        # If anything goes wrong, return None to use default behavior
        return None, None


def wait_for_task(
    task: Union[TaskReference, BaseOperator, str],
    task_id: Optional[str] = None,
    execution_delta: Optional[timedelta] = None,
    execution_date_fn: Optional[Callable] = None,
    auto_reconcile_schedules: bool = True,
    current_dag: Optional[DAG] = None,
        allowed_states: Optional[list] = None,
        failed_states: Optional[list] = None,
        check_existence: bool = True,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,
        soft_fail: bool = False,
        mode: str = 'poke',
        retries: int = 0,
        retry_delay: timedelta = timedelta(minutes=5),
        **kwargs,
) -> WaitForTaskOperator:
    """
    Create a task that waits for another task to complete.

    This is a convenience function that creates a WaitForTaskOperator
    to wait for a task in another DAG. It can automatically reconcile
    schedule differences between DAGs.

    Args:
        task: Can be:
            - A TaskReference object (created via TaskReference(dag_id, task_id))
            - A BaseOperator from another DAG (will extract dag_id and task_id)
            - A string in format "dag_id.task_id"
        task_id: Optional task_id for the wait task. If not provided, will be
            auto-generated as "wait_for_{external_dag_id}_{external_task_id}"
        execution_delta: Time difference with the previous execution to look at.
            If None and auto_reconcile_schedules is True, will be calculated automatically.
        execution_date_fn: Function that receives the current execution date
            and returns the desired execution dates to query. If provided,
            auto_reconcile_schedules is ignored.
        auto_reconcile_schedules: If True (default), automatically calculates
            execution_delta based on schedule differences between DAGs.
            Set to False to disable automatic reconciliation.
        current_dag: The current DAG context. If None, will try to infer from
            context. Should be provided when calling from within a DAG.
        allowed_states: List of allowed states, default is ['success']
        failed_states: List of failed or dis-allowed states
        check_existence: Whether to check if the external task exists
        poke_interval: Time in seconds between each check
        timeout: Time in seconds before the task times out
        soft_fail: Set to true to mark the task as SKIPPED on failure
        mode: How the sensor operates. Options:
            - 'poke' (default): Sensor runs continuously, checking at intervals.
              Keeps the worker slot occupied.
            - 'reschedule': Sensor reschedules itself between checks, freeing
              the worker slot. Better for long waits with infrequent checks.
        retries: Number of times to retry the sensor if it fails (default: 0)
        retry_delay: Time to wait between retries (default: 5 minutes)
        **kwargs: Additional arguments passed to WaitForTaskOperator

    Returns:
        WaitForTaskOperator instance

    Example:
        ```python
        from airflow import DAG
        from waiter import wait_for_task, dags

        with DAG('my_dag', schedule='@daily') as dag:
            # Automatically handles schedule reconciliation
            # If external DAG runs hourly, waits for most recent hourly run
            wait_task = wait_for_task(
                task=dags.hourly_dag.some_task,
                current_dag=dag,  # Provide DAG context for auto-reconciliation
            )
        ```
    """
    # Parse the task parameter
    if isinstance(task, TaskReference):
        external_dag_id = task.dag_id
        external_task_id = task.task_id
    elif isinstance(task, BaseOperator):
        external_dag_id = task.dag.dag_id
        external_task_id = task.task_id
    elif isinstance(task, str):
        if '.' in task:
            external_dag_id, external_task_id = task.split('.', 1)
        else:
            raise ValueError(
                f"String task reference must be in format 'dag_id.task_id', got '{task}'"
            )
    else:
        raise TypeError(
            f"task must be TaskReference, BaseOperator, or str, got {type(task)}"
        )

    # Generate task_id if not provided
    if task_id is None:
        task_id = f"wait_for_{external_dag_id}_{external_task_id}"

    # Auto-reconcile schedules if enabled and execution_delta/execution_date_fn not provided
    if auto_reconcile_schedules and execution_delta is None and execution_date_fn is None:
        # Try to get current DAG from context if not provided
        if current_dag is None:
            # Try to get from kwargs (if passed from DAG context)
            current_dag = kwargs.get('dag')
            # If still None, try to get from contextvars (Airflow 2.x)
            try:
                from airflow.operators.python import get_current_context
                context = get_current_context()
                if context and 'dag' in context:
                    current_dag = context['dag']
            except Exception:
                pass

        if current_dag is not None:
            calculated_delta, calculated_fn = _calculate_execution_delta(current_dag, external_dag_id)
            if calculated_delta is not None:
                execution_delta = calculated_delta
            elif calculated_fn is not None:
                execution_date_fn = calculated_fn

    # Validate mode
    if mode not in ['poke', 'reschedule']:
        raise ValueError(f"mode must be 'poke' or 'reschedule', got '{mode}'")

    return WaitForTaskOperator(
        task_id=task_id,
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
        retries=retries,
        retry_delay=retry_delay,
        **kwargs,
    )

