# Testing Guide

This project includes comprehensive tests for the waiter plugin and DAG validation, all dockerized for consistent execution.

## Quick Start

```bash
# Run all tests in container
make test

# Run specific test suites
make test-unit      # Unit tests only
make test-dags      # DAG import tests only
```

## Test Structure

### Test Files

- **`test_waiter_helpers.py`** - Tests for helper functions (TaskReference, wait_for_task, cron parsing)
- **`test_waiter_operators.py`** - Tests for WaitForTaskOperator
- **`test_waiter_dag_loader.py`** - Tests for DAG loader module (dags.* syntax)
- **`test_dag_imports.py`** - Tests for DAG imports and validation

### Dockerized Testing

Tests run in a containerized environment to ensure:
- Consistent Python and dependency versions
- Isolated test execution
- No local environment setup required
- Easy CI/CD integration

## Running Tests

### Dockerized (Recommended)

```bash
# Build test container (first time or after changes)
make test-build

# Run all tests
make test

# Run specific test suites
make test-unit      # Unit tests for waiter plugin
make test-dags      # DAG import and validation tests

# Run specific test file
podman-compose --profile test run --rm test pytest tests/test_waiter_helpers.py -v

# Run specific test
podman-compose --profile test run --rm test pytest tests/test_waiter_helpers.py::TestWaitForTask::test_wait_for_task_with_string -v

# Open shell in test container for debugging
make test-shell
```

### Local Testing

For local development, you can run tests directly:

```bash
# Install dependencies
pip install -r requirements-test.txt

# Run tests
make test-local
# or
pytest tests/ -v
```

## Test Coverage

The test suite covers:

1. **Waiter Plugin Functionality**
   - TaskReference creation and usage
   - wait_for_task() with various input types
   - Cron expression parsing
   - Schedule reconciliation logic
   - Parameter validation

2. **Operator Functionality**
   - WaitForTaskOperator initialization
   - Mode, retry, and timeout parameters
   - Allowed states configuration

3. **DAG Loader**
   - dags.* syntax for accessing tasks
   - DAGReference and task access

4. **DAG Validation**
   - All DAGs can be imported
   - DAG structure validation
   - Task dependencies validation
   - Waiter plugin usage verification

## Test Results

Test results are saved to `test-results/` directory:
- JUnit XML format for CI/CD integration
- Coverage reports (if enabled)

## CI/CD Integration

The dockerized tests are ready for CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run tests
  run: |
    make test-build
    make test
```

## Debugging Tests

```bash
# Open interactive shell in test container
make test-shell

# Run tests with verbose output
podman-compose --profile test run --rm test pytest tests/ -vv

# Run tests with coverage
podman-compose --profile test run --rm test pytest tests/ --cov=plugins --cov-report=html
```

