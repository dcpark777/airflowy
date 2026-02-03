# When to Manually Calculate execution_delta

The `wait_for_task()` function automatically reconciles schedule differences in most cases. However, there are situations where you'll need to manually specify `execution_delta` or `execution_date_fn`.

## Cases Requiring Manual Calculation

### 1. Cron Expression Schedules

**Status**: ✅ **Now Supported!** The automatic reconciliation can now parse and reconcile cron expressions.

**Example with automatic reconciliation**:
```python
# External DAG runs on cron schedule
external_dag = DAG(
    'external_dag',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
)

# Your DAG
with DAG('my_dag', schedule_interval=timedelta(hours=1)) as dag:
    # Automatic reconciliation works! Finds the most recent 2 AM run
    wait_task = wait_for_task(
        task=dags.external_dag.some_task,
        current_dag=dag,  # Auto-reconciliation enabled by default
    )
```

**Manual override** (if needed):
```python
# If you need specific behavior, you can still override
wait_task = wait_for_task(
    task=dags.external_dag.some_task,
    execution_delta=timedelta(hours=-2),  # Wait for 2 AM run
    auto_reconcile_schedules=False,
)
```

**Note**: For best results with cron expressions, install `croniter`:
```bash
pip install croniter
```

Without `croniter`, the plugin uses basic pattern matching which works for common cron patterns but may not handle all edge cases.

### 2. Manual/Triggered DAGs

**Problem**: DAGs with `schedule_interval=None` (manually triggered) have no schedule to reconcile.

**Example**:
```python
# External DAG is manually triggered
external_dag = DAG(
    'external_dag',
    schedule_interval=None,  # Manual trigger only
)

# Your scheduled DAG
with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    # Must specify which run to wait for
    wait_task = wait_for_task(
        task=dags.external_dag.some_task,
        execution_delta=timedelta(days=-1),  # Wait for yesterday's manual run
        auto_reconcile_schedules=False,
    )
```

### 3. Complex Custom Timetables

**Problem**: Custom timetable objects without a simple `delta` attribute cannot be auto-calculated.

**Example**:
```python
from airflow.timetables.interval import CronDataIntervalTimetable

# External DAG uses complex timetable
external_dag = DAG(
    'external_dag',
    schedule_interval=CronDataIntervalTimetable('0 0 * * 1'),  # Weekly on Monday
)

# Your DAG
with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    # Must manually calculate
    wait_task = wait_for_task(
        task=dags.external_dag.some_task,
        execution_date_fn=lambda dt: dt.replace(day=1),  # Wait for first Monday of month
        auto_reconcile_schedules=False,
    )
```

### 4. Waiting for a Specific Previous Run

**Problem**: You want to wait for a run from N periods ago, not just the most recent one.

**Example**:
```python
# Both DAGs run daily
with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    # Wait for the run from 2 days ago, not yesterday
    wait_task = wait_for_task(
        task=dags.external_dag.some_task,
        execution_delta=timedelta(days=-2),  # 2 days ago
        auto_reconcile_schedules=False,
    )
```

### 5. Business Logic Requirements

**Problem**: Automatic calculation doesn't match your business requirements.

**Example**:
```python
# External DAG runs hourly, your DAG runs daily
# But you need to wait for the run from 3 hours ago, not the most recent
with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    wait_task = wait_for_task(
        task=dags.hourly_dag.some_task,
        execution_delta=timedelta(hours=-3),  # Specific business requirement
        auto_reconcile_schedules=False,
    )
```

### 6. DAG Not Found at Parse Time

**Problem**: The external DAG might not be loaded yet when your DAG is parsed.

**Example**:
```python
# External DAG might not exist yet or isn't in the DAG bag
with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    # Provide manual delta as fallback
    wait_task = wait_for_task(
        task='external_dag.some_task',
        execution_delta=timedelta(days=-1),  # Manual fallback
        auto_reconcile_schedules=True,  # Will try auto, fall back to manual if needed
    )
```

### 7. Custom execution_date_fn Requirements

**Problem**: You need complex logic to determine which execution date to wait for.

**Example**:
```python
def get_last_business_day(execution_date: datetime) -> datetime:
    """Get the last business day (Mon-Fri) before execution_date."""
    # Skip weekends
    days_back = 1
    while execution_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        execution_date -= timedelta(days=days_back)
        days_back += 1
    return execution_date - timedelta(days=1)

with DAG('my_dag', schedule_interval=timedelta(days=1)) as dag:
    wait_task = wait_for_task(
        task=dags.external_dag.some_task,
        execution_date_fn=get_last_business_day,
        auto_reconcile_schedules=False,
    )
```

## Summary

**Use automatic reconciliation when:**
- Both DAGs use simple `timedelta` schedules
- You want to wait for the most recent run
- Schedules are compatible (one is a multiple of the other)

**Manually specify execution_delta/execution_date_fn when:**
- Using cron expressions
- Dealing with manual/triggered DAGs
- Using complex custom timetables
- You need a specific previous run (not most recent)
- Business logic requires custom timing
- External DAG might not be loaded yet
- You need complex execution date calculation logic

## Best Practice

Always provide `current_dag` when possible, and let auto-reconciliation try first. If it fails or doesn't match your needs, disable it and provide manual values:

```python
wait_task = wait_for_task(
    task=dags.external_dag.some_task,
    current_dag=dag,
    auto_reconcile_schedules=True,  # Try automatic first
    execution_delta=timedelta(days=-1),  # Fallback if auto fails
)
```

