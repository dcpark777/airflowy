# Example DAGs

This directory contains example DAGs demonstrating various features and plugins.

## Included Examples

- **`simple_dag.py`** - A basic example DAG with simple Bash tasks
- **`example_wait_dag.py`** - Demonstrates the waiter plugin for waiting on tasks from other DAGs
- **`example_cron_wait_dag.py`** - Shows cron expression reconciliation with the waiter plugin
- **`example_sql_sensor_dag.py`** - Demonstrates the SQL sensor plugin for waiting on SQL query results
- **`demo_hourly_dag.py`** - A demo DAG that runs hourly, used for schedule reconciliation examples

## Enabling Example DAGs

By default, example DAGs are **disabled** to avoid cluttering your Airflow UI. They are ignored via the `.airflowignore` file in the parent `dags/` directory.

To enable example DAGs:

1. **Edit `.airflowignore`** in the `dags/` directory:
   ```bash
   # Comment out or remove the 'examples/' line
   # examples/
   ```

2. **Or remove the `.airflowignore` file entirely** if you want to load all DAGs.

3. **Restart Airflow** for the changes to take effect.

## Usage

These example DAGs are provided for:
- Learning how to use the custom plugins (waiter, sensors)
- Understanding DAG structure and best practices
- Testing plugin functionality
- Reference implementations

**Note:** These are example DAGs and may not be suitable for production use. Review and modify them according to your needs.

