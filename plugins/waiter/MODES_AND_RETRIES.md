# Wait Modes and Retry Behavior

The `wait_for_task()` function supports configurable retry behavior and different wait modes to suit various use cases.

## Wait Modes

### Poke Mode (Default)

**Mode**: `mode='poke'`

In poke mode, the sensor runs continuously, checking at the specified `poke_interval`. The worker slot remains occupied during the entire wait period.

**Use when:**
- You expect the wait to be relatively short
- You want immediate checking without rescheduling overhead
- Worker resources are not a concern

**Example:**
```python
wait_task = wait_for_task(
    task=dags.other_dag.some_task,
    mode='poke',
    poke_interval=60,  # Check every minute
)
```

**Pros:**
- Lower latency - checks happen immediately
- Simpler execution model
- Good for short waits (< 1 hour)

**Cons:**
- Occupies worker slot during entire wait
- Can block other tasks if workers are limited
- Not ideal for very long waits

### Reschedule Mode

**Mode**: `mode='reschedule'`

In reschedule mode, the sensor reschedules itself between checks, freeing the worker slot. This allows other tasks to run while waiting.

**Use when:**
- You expect long wait times
- Worker resources are limited
- You have infrequent checks (e.g., every 5+ minutes)
- You want to maximize worker utilization

**Example:**
```python
wait_task = wait_for_task(
    task=dags.other_dag.some_task,
    mode='reschedule',
    poke_interval=300,  # Check every 5 minutes
)
```

**Pros:**
- Frees worker slot between checks
- Better resource utilization
- Ideal for long waits
- Allows other tasks to run

**Cons:**
- Slight overhead from rescheduling
- Slightly higher latency (rescheduling delay)
- Best with longer poke intervals

## Retry Behavior

### Configuring Retries

You can configure how many times the sensor should retry if it fails, and how long to wait between retries.

**Parameters:**
- `retries`: Number of times to retry (default: `0`)
- `retry_delay`: Time to wait between retries (default: `5 minutes`)

**Example:**
```python
wait_task = wait_for_task(
    task=dags.other_dag.some_task,
    retries=3,  # Retry up to 3 times
    retry_delay=timedelta(minutes=10),  # Wait 10 minutes between retries
)
```

### Retry Scenarios

Retries are useful when:
- The external task might be delayed
- Network issues might cause temporary failures
- The external DAG might not be ready yet
- You want to give the external task more time

**Example with retries:**
```python
from datetime import timedelta

wait_task = wait_for_task(
    task=dags.data_processing_dag.load_data,
    mode='reschedule',
    poke_interval=600,  # Check every 10 minutes
    timeout=3600 * 24,  # Timeout after 24 hours
    retries=5,  # Retry up to 5 times
    retry_delay=timedelta(minutes=15),  # Wait 15 minutes between retries
)
```

## Choosing the Right Configuration

### Short Wait (< 1 hour), Frequent Checks

```python
wait_task = wait_for_task(
    task=dags.quick_dag.task,
    mode='poke',
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # 1 hour timeout
    retries=1,
    retry_delay=timedelta(minutes=2),
)
```

### Long Wait (> 1 hour), Infrequent Checks

```python
wait_task = wait_for_task(
    task=dags.long_running_dag.task,
    mode='reschedule',
    poke_interval=1800,  # Check every 30 minutes
    timeout=3600 * 48,  # 48 hour timeout
    retries=3,
    retry_delay=timedelta(minutes=30),
)
```

### Critical Dependency, Multiple Retries

```python
wait_task = wait_for_task(
    task=dags.critical_dag.task,
    mode='poke',
    poke_interval=60,
    timeout=3600 * 12,  # 12 hour timeout
    retries=10,  # Many retries for critical dependency
    retry_delay=timedelta(minutes=5),
)
```

### Resource-Constrained Environment

```python
wait_task = wait_for_task(
    task=dags.other_dag.task,
    mode='reschedule',  # Free worker slot
    poke_interval=600,  # Check every 10 minutes
    retries=2,
    retry_delay=timedelta(minutes=10),
)
```

## Complete Example

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

    # Wait for external task with reschedule mode and retries
    wait_task = wait_for_task(
        task=dags.external_dag.critical_task,
        current_dag=dag,
        mode='reschedule',  # Free worker between checks
        poke_interval=300,  # Check every 5 minutes
        timeout=3600 * 24,  # 24 hour timeout
        retries=5,  # Retry up to 5 times
        retry_delay=timedelta(minutes=10),  # 10 minutes between retries
    )

    # Process after waiting
    process_task = BashOperator(
        task_id='process',
        bash_command='echo "Processing after wait"',
    )

    wait_task >> process_task
```

## Summary

- **Poke mode**: Use for short waits, frequent checks, when worker resources aren't a concern
- **Reschedule mode**: Use for long waits, infrequent checks, when you want to maximize worker utilization
- **Retries**: Configure based on how critical the dependency is and expected variability
- **Retry delay**: Set based on how long you expect issues to resolve

