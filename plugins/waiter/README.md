# Waiter Plugin

A helper plugin for waiting on tasks from other DAGs in Airflow.

## Features

- Easy-to-use API for waiting on external tasks
- Support for multiple ways to reference tasks
- Convenient `dags` module for accessing tasks from other DAGs
- **Automatic schedule reconciliation** - handles different schedules between DAGs automatically, including cron expressions
- **Configurable retry behavior** - control how many times to retry and delay between retries
- **Flexible wait modes** - choose between 'poke' (continuous) or 'reschedule' (free worker slot) modes

## Optional Dependencies

For best cron expression support, install `croniter`:
```bash
pip install croniter
```

Without `croniter`, the plugin uses basic pattern matching which works for common cron patterns (hourly, daily, weekly, etc.) but may not handle all edge cases. With `croniter`, full cron expression parsing is available.

## Usage

### Basic Usage

```python
from waiter import wait_for_task, dags

# Wait for a task from another DAG
wait_task = wait_for_task(task=dags.my_dag.my_task_id)
```

### Using String Format

```python
from waiter import wait_for_task

# Wait using string format: "dag_id.task_id"
wait_task = wait_for_task(task='my_dag.my_task_id')
```

### Using TaskReference

```python
from waiter import wait_for_task, TaskReference

# Create a task reference
my_task_ref = TaskReference(dag_id='my_dag', task_id='my_task_id')

# Wait for it
wait_task = wait_for_task(task=my_task_ref)
```

### Complete Example

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from waiter import wait_for_task, dags

with DAG(
    'my_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(days=1),
) as dag:

    # Wait for a task from another DAG
    # Schedule reconciliation is automatic - if other_dag runs hourly,
    # it will wait for the most recent hourly run
    wait_task = wait_for_task(
        task=dags.other_dag.some_task,
        task_id='wait_for_other_dag',
        current_dag=dag,  # Provide DAG context for auto-reconciliation
        mode='reschedule',  # Free worker slot between checks
        retries=2,  # Retry up to 2 times
        retry_delay=timedelta(minutes=5),
        poke_interval=300,  # Check every 5 minutes
    )

    # Your task that runs after waiting
    my_task = BashOperator(
        task_id='my_task',
        bash_command='echo "Running after waiting"',
    )

    wait_task >> my_task
```

### Schedule Reconciliation

The plugin automatically handles schedule differences between DAGs, including cron expressions:

- **Same schedules**: Waits for the same execution date
- **Current DAG runs less frequently** (e.g., daily) waiting for **external DAG that runs more frequently** (e.g., hourly):
  - Waits for the most recent hourly run at or before the current execution date
- **Current DAG runs more frequently** (e.g., hourly) waiting for **external DAG that runs less frequently** (e.g., daily):
  - Waits for the daily run that contains the current execution time
- **Cron expressions**: Automatically parsed and reconciled
  - Example: Hourly DAG waiting for `'0 2 * * *'` (daily at 2 AM) will wait for the most recent 2 AM run
  - Works with or without `croniter` library (better accuracy with `croniter`)

To disable automatic reconciliation, set `auto_reconcile_schedules=False`:

```python
wait_task = wait_for_task(
    task=dags.other_dag.some_task,
    auto_reconcile_schedules=False,
    execution_delta=timedelta(hours=1),  # Manual delta
)
```

**When do you need to manually calculate `execution_delta`?**

See [SCHEDULE_RECONCILIATION.md](SCHEDULE_RECONCILIATION.md) for detailed information about cases where automatic reconciliation doesn't work, including:
- ~~Cron expression schedules~~ ✅ **Now supported!**
- Manual/triggered DAGs
- Complex custom timetables
- Waiting for specific previous runs
- Custom business logic requirements

**Wait modes and retry behavior:**

See [MODES_AND_RETRIES.md](MODES_AND_RETRIES.md) for detailed information about:
- Poke vs reschedule modes
- Configuring retry behavior
- Choosing the right configuration for your use case

## API Reference

### `wait_for_task(task, **kwargs)`

Creates a `WaitForTaskOperator` that waits for a task in another DAG.

**Parameters:**
- `task`: Can be:
  - A `TaskReference` object
  - A `BaseOperator` from another DAG
  - A string in format `"dag_id.task_id"`
- `task_id`: Optional task ID for the wait task (auto-generated if not provided)
- `execution_delta`: Time difference with previous execution (optional, auto-calculated if not provided)
- `execution_date_fn`: Function to determine execution dates to query (optional, auto-calculated if not provided)
- `auto_reconcile_schedules`: Automatically calculate execution_delta based on schedule differences (default: `True`)
- `current_dag`: The current DAG context for schedule reconciliation (optional, auto-detected if possible)
- `allowed_states`: List of allowed states (default: `['success']`)
- `failed_states`: List of failed states (optional)
- `check_existence`: Whether to check if external task exists (default: `True`)
- `poke_interval`: Time in seconds between checks (default: `60`)
- `timeout`: Time in seconds before timeout (default: `7 days`)
- `soft_fail`: Mark task as SKIPPED on failure (default: `False`)
- `mode`: How the sensor operates (default: `'poke'`):
  - `'poke'`: Sensor runs continuously, checking at intervals. Keeps worker slot occupied.
  - `'reschedule'`: Sensor reschedules itself between checks, freeing worker slot. Better for long waits.
- `retries`: Number of times to retry the sensor if it fails (default: `0`)
- `retry_delay`: Time to wait between retries (default: `5 minutes`)

**Returns:** `WaitForTaskOperator` instance

### `dags`

Module-like object for accessing tasks from other DAGs.

```python
from waiter import dags

# Access a task: dags.dag_id.task_id
my_task = dags.my_dag.my_task_id
```

### `TaskReference(dag_id, task_id)`

Creates a reference to a task in another DAG.

### `WaitForTaskOperator`

Custom operator that extends `ExternalTaskSensor`. See `operators.py` for full documentation.

