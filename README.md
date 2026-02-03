# Airflowy

Local Airflow development environment using Podman Compose.

## Prerequisites

- Podman installed and running
- Podman machine with at least 4GB RAM (check with `make check-resources`)

## Quick Start

1. **Initialize Airflow** (first time only):

   ```bash
   make init
   ```

2. **Start Airflow**:

   ```bash
   make run
   ```

3. **Access the web UI**:

   - URL: <http://localhost:8080>
   - Username: `airflow`
   - Password: `airflow`

## Available Commands

Run `make` or `make help` to see all available commands:

- `make run` - Start all containers
- `make stop` - Stop containers
- `make clean` - Stop and remove containers
- `make logs` - View logs (use `LOGS_SERVICE=<name>` for specific service)
- `make status` - Show container status
- `make check-resources` - Check Podman machine resources
- `make fix-resources` - Show instructions to increase memory

## Directory Structure

- `dags/` - Place your DAG files here
  - `dags/examples/` - Example DAGs (disabled by default via `.airflowignore`)
  - `dags/templates/` - DAG templates for quick creation
  - `dags/{tenant}/` - Tenant-specific DAG directories (recommended)
- `logs/` - Airflow logs
- `plugins/` - Airflow plugins
  - `plugins/tenants/` - Tenant management utilities
- `scripts/` - DAG management and validation scripts
- `tests/` - Test files for the project

## Waiter Plugin

This project includes a custom plugin for waiting on tasks from other DAGs.

### Quick Example

```python
from waiter import wait_for_task, dags

# Wait for a task from another DAG
wait_task = wait_for_task(task=dags.my_dag.my_task_id)
```

See `plugins/waiter/README.md` for full documentation and `dags/examples/example_wait_dag.py` for a complete example.

**Note:** Example DAGs are in the `dags/examples/` directory and are disabled by default. See `dags/examples/README.md` for instructions on enabling them.

## Multi-Tenant Development

This repository is designed for multi-tenant use with built-in guardrails to prevent issues.

### Quick Onboarding

1. **Create your first DAG:**
   ```bash
   python scripts/create_dag.py --name my_dag --tenant my-team
   ```

2. **Test your DAG:**
   ```bash
   make test-dags
   make test-dag-best-practices
   ```

3. **Start developing!**

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### DAG Naming Convention

All DAGs must follow the format: `{tenant}_{dag_name}`

**Examples:**
- ✅ `data-engineering_daily_etl`
- ✅ `analytics_weekly_report`
- ❌ `my_dag` (missing tenant prefix)

### Tenant Isolation

- **DAG IDs**: Must include tenant prefix
- **Connections**: Use format `{tenant}_{connection_name}`
- **Variables**: Use format `{tenant}_{variable_name}`
- **Directories**: Organize DAGs by tenant (optional but recommended)

### Resource Limits

All DAGs must specify resource limits to prevent resource exhaustion:
- `max_active_runs`: Maximum concurrent DAG runs (default: 1, max: 5)
- `max_active_tasks`: Maximum concurrent tasks (default: 3, max: 10)

### Validation & Guardrails

The repository includes automatic validation:

- **Pre-commit hooks**: Validate DAGs before commit
- **CI/CD checks**: Automatic validation on PR
- **Naming conventions**: Enforced tenant prefix format
- **Resource limits**: Prevent resource exhaustion
- **Best practices**: Owner, docs, schedule, etc.

**Install pre-commit hooks:**
```bash
pip install pre-commit
pre-commit install
```

**Run validation manually:**
```bash
# Validate DAG syntax
python scripts/validate_dags.py

# Check naming conventions
python scripts/check_dag_naming.py

# Check resource limits
python scripts/check_dag_resources.py
```

## Testing

Tests are dockerized and can be run in containers (recommended) or locally.

### Dockerized Tests (Recommended)

```bash
# Run all tests in container
make test

# Run only unit tests
make test-unit

# Test DAG imports
make test-dags

# Test DAG best practices (owner, documentation, etc.)
make test-dag-best-practices

# Build test container
make test-build

# Open shell in test container
make test-shell
```

### Local Tests

For local testing, install dependencies first:

```bash
pip install -r requirements-test.txt
make test-local
```

The dockerized tests ensure a consistent environment and don't require local Python setup.

## Creating a New DAG

### Option 1: Use the DAG Generator (Recommended)

```bash
python scripts/create_dag.py \
  --name daily_etl \
  --tenant data-engineering \
  --schedule "0 2 * * *" \
  --description "Daily ETL pipeline"
```

This creates a properly formatted DAG with all required fields.

### Option 2: Copy from Template

```bash
cp dags/templates/dag_template.py dags/my-team/my_dag.py
# Edit the file and replace placeholders
```

### Option 3: Start from Scratch

Follow the DAG requirements checklist in [CONTRIBUTING.md](CONTRIBUTING.md).

## DAG Best Practices

All DAGs in this project are validated against best practices. The `test-dag-best-practices` test suite ensures:

- **Owner**: Every DAG must have an owner set (in `default_args` or DAG constructor)
- **Documentation**: Every DAG must have documentation (module docstring, `description`, or `doc_md`)
- **Tags**: DAGs should have tags for organization (recommended)
- **Schedule**: Schedule must be explicitly set (can be `None` for manual DAGs)
- **Catchup**: `catchup` must be explicitly set to `True` or `False`
- **Start Date**: Every DAG must have a `start_date` set
- **Retries**: Retries must be configured (can be 0)
- **Retry Delay**: If retries > 0, `retry_delay` must be configured
- **Max Active Runs**: `max_active_runs` should be set (recommended)

Example DAG following best practices:

```python
"""
Example DAG demonstrating best practices.

This DAG shows how to structure a DAG with all required fields.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineering',  # Required
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  # Required
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Required
    'retry_delay': timedelta(minutes=5),  # Required if retries > 0
}

with DAG(
    'data-engineering_example_dag',  # Tenant prefix required
    default_args=default_args,
    description='Example DAG following best practices',  # Documentation
    schedule=timedelta(days=1),  # Explicitly set
    catchup=False,  # Explicitly set
    max_active_runs=1,  # Required (prevents resource exhaustion)
    max_active_tasks=3,  # Required (prevents resource exhaustion)
    tags=['data-engineering', 'example'],  # Recommended (include tenant tag)
) as dag:
    task = BashOperator(
        task_id='example_task',
        bash_command='echo "Hello World"',
    )
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development workflow
- DAG requirements checklist
- Tenant isolation guidelines
- Pull request process
- Common issues and solutions

## Troubleshooting

### Webserver not responding

1. Check resources: `make check-resources`
2. Increase memory if needed: `make fix-resources`
3. Restart: `make restart`

### DAG not appearing in UI

1. Check `.airflowignore` - your DAG might be ignored
2. Check DAG syntax: `make test-dags`
3. Check Airflow logs: `make logs LOGS_SERVICE=airflow-scheduler`

### Validation errors

1. **"DAG missing owner"**: Add `owner` to `default_args`
2. **"DAG missing documentation"**: Add module docstring or `description`
3. **"Invalid DAG naming"**: Use format `{tenant}_{dag_name}`
4. **"Resource limits not set"**: Add `max_active_runs` and `max_active_tasks`
