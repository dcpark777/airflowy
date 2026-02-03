# Validation Scripts

This directory contains scripts for validating and managing DAGs in a multi-tenant Airflow environment.

## Scripts

### `validate_dags.py`

Validates DAG files for syntax errors and import issues.

```bash
python scripts/validate_dags.py
python scripts/validate_dags.py dags/my_team/my_dag.py
```

### `check_dag_naming.py`

Validates that DAGs follow the tenant naming convention: `{tenant}_{dag_name}`

```bash
python scripts/check_dag_naming.py
python scripts/check_dag_naming.py dags/my_team/my_dag.py
```

### `check_dag_resources.py`

Validates that DAGs have proper resource limits configured.

```bash
python scripts/check_dag_resources.py
python scripts/check_dag_resources.py dags/my_team/my_dag.py
```

### `create_dag.py`

Generates a new DAG file from a template with proper tenant isolation.

```bash
python scripts/create_dag.py --name my_dag --tenant my-team
python scripts/create_dag.py --name daily_etl --tenant data-engineering --schedule "0 2 * * *"
```

## Shared Utilities

### `common.py`

Common utilities used by all validation scripts:
- `setup_airflow_environment()` - Configure Airflow environment
- `find_dag_files()` - Find all DAG files
- `run_validation()` - Run validation with consistent output
- `print_errors()` / `print_warnings()` - Consistent error formatting

### `dag_parser.py`

Utilities for parsing DAG files:
- `extract_dag_config_from_ast()` - Extract config via AST parsing
- `extract_dag_ids_from_ast()` - Extract DAG IDs via AST
- `load_dag_from_dagbag()` - Load DAG via Airflow DagBag
- `get_dag_info()` - Get complete DAG info (combines AST + DagBag)

## Architecture

The scripts follow a consistent pattern:


1. **Setup**: Configure Airflow environment and paths
2. **Validation**: Check files using shared utilities
3. **Output**: Print results in consistent format

This reduces code duplication and makes the scripts easier to maintain.

## Usage in CI/CD

These scripts are used in:

- Pre-commit hooks (`.pre-commit-config.yaml`)
- GitHub Actions (`.github/workflows/ci.yml`)
- Makefile targets (`make validate-dags`, etc.)

