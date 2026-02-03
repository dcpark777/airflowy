# Contributing to Airflowy

Welcome! This guide will help you get started contributing to this multi-tenant Airflow repository.

## Quick Start

1. **Fork and clone the repository**
2. **Set up your development environment:**
   ```bash
   make init
   make run
   ```
3. **Create your DAG following the template:**
   ```bash
   python scripts/create_dag.py --name my_dag --tenant my_team
   ```
4. **Test your DAG:**
   ```bash
   make test-dags
   make test-dag-best-practices
   ```
5. **Submit a pull request**

## Development Workflow

### 1. Create a New DAG

**Option A: Use the DAG template (Recommended)**
```bash
python scripts/create_dag.py \
  --name my_data_pipeline \
  --tenant data-engineering \
  --schedule "0 2 * * *" \
  --description "Process daily data"
```

**Option B: Copy from template manually**
```bash
cp dags/templates/dag_template.py dags/my_team/my_data_pipeline.py
```

### 2. DAG Naming Conventions

All DAGs must follow the naming convention:
- Format: `{tenant}_{dag_name}`
- Tenant: Your team/department identifier (e.g., `data-engineering`, `analytics`, `ml-team`)
- DAG name: Descriptive name in snake_case (e.g., `daily_etl`, `model_training`)

**Examples:**
- ✅ `data-engineering_daily_etl`
- ✅ `analytics_weekly_report`
- ❌ `my_dag` (missing tenant prefix)
- ❌ `DataPipeline` (wrong format)

### 3. Tenant Isolation

Each tenant should:
- Use tenant-prefixed DAG IDs
- Use tenant-specific connections (format: `{tenant}_{connection_name}`)
- Use tenant-specific variables (format: `{tenant}_{variable_name}`)
- Place DAGs in tenant-specific directories (optional but recommended)

**Directory Structure:**
```
dags/
  {tenant}/
    dag1.py
    dag2.py
```

### 4. Resource Limits

To prevent resource exhaustion, all DAGs must specify:
- `max_active_runs`: Maximum concurrent DAG runs (default: 1)
- `max_active_tasks`: Maximum concurrent tasks per DAG run (default: 3)
- Task-level resource limits (if applicable)

**Example:**
```python
with DAG(
    'my_tenant_my_dag',
    max_active_runs=1,
    max_active_tasks=3,
    ...
) as dag:
    ...
```

### 5. Testing Your DAG

Before submitting, ensure your DAG passes all checks:

```bash
# Test DAG imports (syntax, imports)
make test-dags

# Test best practices (owner, docs, etc.)
make test-dag-best-practices

# Run pre-commit hooks (if installed)
pre-commit run --all-files
```

### 6. Code Quality

- Follow PEP 8 style guidelines
- Add docstrings to all functions and classes
- Keep functions small and focused
- Use type hints where possible
- Add comments for complex logic

## DAG Requirements Checklist

Before submitting a DAG, ensure it has:

- [ ] **Tenant prefix** in DAG ID (format: `{tenant}_{name}`)
- [ ] **Owner** set in `default_args` or DAG constructor
- [ ] **Documentation** (module docstring, `description`, or `doc_md`)
- [ ] **Tags** for organization (at least one tag)
- [ ] **Explicit schedule** (can be `None` for manual DAGs)
- [ ] **Explicit catchup** (`True` or `False`)
- [ ] **Start date** set
- [ ] **Retries** configured (can be 0)
- [ ] **Retry delay** configured (if retries > 0)
- [ ] **Max active runs** set (recommended: 1)
- [ ] **Max active tasks** set (recommended: 3)
- [ ] **Resource limits** if using resource-intensive tasks
- [ ] **Tests** for complex logic (if applicable)

## Connection and Secret Management

### Creating Connections

1. Use tenant-prefixed connection IDs: `{tenant}_{connection_name}`
2. Document connection requirements in DAG docstring
3. Never commit credentials to the repository

**Example:**
```python
# In DAG docstring:
"""
This DAG requires the following connection:
- Connection ID: data-engineering_postgres_prod
- Connection Type: Postgres
- Host: your-db-host
- Schema: your-database
"""
```

### Using Secrets

- Use Airflow Variables for non-sensitive configuration
- Use Airflow Connections for credentials
- Use external secret managers (AWS Secrets Manager, Vault) for production
- Never hardcode credentials

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b tenant/feature-name
   ```

2. **Make your changes:**
   - Follow DAG requirements checklist
   - Add tests if applicable
   - Update documentation if needed

3. **Test locally:**
   ```bash
   make test
   ```

4. **Commit your changes:**
   ```bash
   git commit -m "Add tenant_dag_name DAG"
   ```
   (Pre-commit hooks will run automatically)

5. **Push and create PR:**
   ```bash
   git push origin tenant/feature-name
   ```

6. **PR will be automatically validated:**
   - DAG import tests
   - Best practices validation
   - Code quality checks

## Common Issues and Solutions

### Issue: DAG not appearing in UI

**Solutions:**
- Check `.airflowignore` - your DAG might be ignored
- Check DAG syntax errors: `make test-dags`
- Check Airflow logs: `make logs LOGS_SERVICE=airflow-scheduler`

### Issue: "DAG missing owner" error

**Solution:**
Add owner to `default_args`:
```python
default_args = {
    'owner': 'your-team-name',
    ...
}
```

### Issue: "DAG missing documentation" error

**Solution:**
Add a module docstring or DAG description:
```python
"""
Your DAG description here.
"""
```

### Issue: Resource exhaustion

**Solution:**
Add resource limits:
```python
with DAG(
    'my_dag',
    max_active_runs=1,
    max_active_tasks=3,
    ...
) as dag:
    ...
```

## Getting Help

- Check the [README.md](README.md) for setup instructions
- Review example DAGs in `dags/examples/`
- Check Airflow logs: `make logs`
- Ask questions in your team's communication channel

## Code of Conduct

- Be respectful and inclusive
- Help others learn and contribute
- Follow best practices and guidelines
- Test your changes before submitting
- Keep DAGs simple and maintainable

## Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [DAG Best Practices](README.md#dag-best-practices)
- [Testing Guide](tests/README.md)
- [Waiter Plugin Documentation](plugins/waiter/README.md)

