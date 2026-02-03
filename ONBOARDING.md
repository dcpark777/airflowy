# Quick Onboarding Guide

Welcome! This guide will get you up and running in 5 minutes.

## Step 1: Set Up Your Environment

```bash
# Clone the repository
git clone <repository-url>
cd airflowy

# Initialize Airflow (first time only)
make init

# Start Airflow
make run
```

Access the UI at http://localhost:8080 (username: `airflow`, password: `airflow`)

## Step 2: Create Your First DAG

Use the DAG generator to create a properly formatted DAG:

```bash
python scripts/create_dag.py \
  --name my_first_dag \
  --tenant my-team \
  --schedule "0 2 * * *" \
  --description "My first DAG"
```

This creates: `dags/my-team/my_first_dag.py`

## Step 3: Edit Your DAG

Open the generated DAG file and add your tasks:

```python
# In dags/my-team/my_first_dag.py

with DAG(...) as dag:
    # Your tasks here
    task1 = BashOperator(...)
    task2 = PythonOperator(...)
    
    task1 >> task2
```

## Step 4: Test Your DAG

```bash
# Validate syntax
make validate-dags

# Check naming
make check-naming

# Check resource limits
make check-resources

# Run all tests
make test
```

## Step 5: Install Pre-commit Hooks (Optional but Recommended)

```bash
make install-precommit
```

This automatically validates your DAGs before each commit.

## Step 6: Submit Your DAG

```bash
git add dags/my-team/my_first_dag.py
git commit -m "Add my-team_my_first_dag"
git push
```

## Common Tasks

### View DAGs in UI
- Open http://localhost:8080
- Find your DAG: `my-team_my_first_dag`

### Check Logs
```bash
make logs LOGS_SERVICE=airflow-scheduler
```

### Stop Airflow
```bash
make stop
```

### Restart Airflow
```bash
make restart
```

## Need Help?

- **Full guide**: See [CONTRIBUTING.md](CONTRIBUTING.md)
- **DAG examples**: Check `dags/examples/`
- **Troubleshooting**: See [README.md](README.md#troubleshooting)

## Next Steps

1. ✅ Create your first DAG
2. ✅ Test it locally
3. ✅ Submit a PR
4. 📚 Read [CONTRIBUTING.md](CONTRIBUTING.md) for best practices
5. 🔧 Explore the plugins in `plugins/`

Happy coding! 🚀

