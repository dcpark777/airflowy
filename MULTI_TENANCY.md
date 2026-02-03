# Multi-Tenancy Features

This document describes the multi-tenancy features and guardrails implemented in this Airflow repository.

## Overview

This repository is designed to support multiple tenants (teams/departments) working in the same Airflow instance with proper isolation and guardrails to prevent issues.

## Key Features

### 1. Tenant Isolation

**DAG Naming Convention**
- All DAGs must follow format: `{tenant}_{dag_name}`
- Example: `data-engineering_daily_etl`
- Enforced via validation scripts and CI/CD

**Connection & Variable Scoping**
- Connections: `{tenant}_{connection_name}`
- Variables: `{tenant}_{variable_name}`
- Helps prevent conflicts between tenants

**Directory Organization**
- Recommended: Organize DAGs by tenant in `dags/{tenant}/`
- Optional but helps with organization

### 2. Resource Limits

**Automatic Enforcement**
- `max_active_runs`: Default 1, max 5
- `max_active_tasks`: Default 3, max 10
- Prevents resource exhaustion from runaway DAGs

**Validation**
- Scripts check resource limits are set
- CI/CD enforces limits
- Warnings for limits above recommended values

### 3. Validation & Guardrails

**Pre-commit Hooks**
- Validates DAG syntax before commit
- Checks naming conventions
- Verifies resource limits
- Runs code quality checks (black, flake8, isort)

**CI/CD Checks**
- Automatic validation on every PR
- DAG import tests
- Best practices validation
- Naming convention checks
- Resource limit checks

**Validation Scripts**
- `scripts/validate_dags.py` - Syntax and import validation
- `scripts/check_dag_naming.py` - Naming convention validation
- `scripts/check_dag_resources.py` - Resource limit validation

### 4. Quick Onboarding

**DAG Generator**
```bash
python scripts/create_dag.py --name my_dag --tenant my-team
```
- Creates properly formatted DAG
- Includes all required fields
- Follows best practices automatically

**Templates**
- `dags/templates/dag_template.py` - Copy and customize
- Includes all required fields with placeholders

**Documentation**
- `ONBOARDING.md` - 5-minute quick start
- `CONTRIBUTING.md` - Detailed guidelines
- `README.md` - Setup and usage

### 5. Tenant Utilities

**Plugin: `plugins/tenants/`**
- `get_tenant_from_dag_id()` - Extract tenant from DAG ID
- `validate_tenant()` - Validate tenant format
- `get_connection_for_tenant()` - Get tenant-scoped connection
- `get_variable_for_tenant()` - Get tenant-scoped variable
- `ensure_tenant_isolation()` - Enforce tenant isolation

### 6. Best Practices Enforcement

**Required Fields**
- Owner (tenant/team name)
- Documentation (docstring or description)
- Tags (for organization)
- Explicit schedule
- Explicit catchup
- Start date
- Retries configuration
- Resource limits

**Automatic Testing**
- `make test-dag-best-practices` - Validates all DAGs
- Runs in CI/CD automatically
- Clear error messages for violations

## Usage Examples

### Creating a New DAG

```bash
# Generate DAG
python scripts/create_dag.py \
  --name daily_etl \
  --tenant data-engineering \
  --schedule "0 2 * * *"

# Edit the generated file
vim dags/data-engineering/daily_etl.py

# Test
make validate-dags
make test-dag-best-practices
```

### Using Tenant Utilities

```python
from plugins.tenants import get_tenant_from_dag_id, get_connection_for_tenant

# Extract tenant from DAG ID
tenant = get_tenant_from_dag_id('data-engineering_daily_etl')
# Returns: 'data-engineering'

# Get tenant-scoped connection
conn = get_connection_for_tenant('data-engineering', 'postgres_prod')
# Uses connection ID: 'data-engineering_postgres_prod'
```

### Validating DAGs

```bash
# Validate all DAGs
make validate-dags

# Check naming
make check-naming

# Check resources
make check-resources

# Run all checks
make test
```

## Configuration

### Known Tenants

Edit `plugins/tenants/helpers.py` to add known tenants:

```python
KNOWN_TENANTS = {
    'data-engineering',
    'analytics',
    'ml-team',
    # Add your tenants here
}
```

### Resource Limits

Edit validation scripts to adjust limits:

- `scripts/check_dag_resources.py` - Default and max limits
- `DEFAULT_MAX_ACTIVE_RUNS = 1`
- `MAX_ALLOWED_ACTIVE_RUNS = 5`

## Benefits

1. **Safety**: Guardrails prevent breaking changes
2. **Isolation**: Tenants don't interfere with each other
3. **Consistency**: All DAGs follow same patterns
4. **Quick Onboarding**: New developers productive in minutes
5. **Quality**: Automatic validation ensures best practices
6. **Scalability**: Easy to add new tenants

## Migration Guide

### Existing DAGs

If you have existing DAGs without tenant prefixes:

1. Rename DAG ID: `my_dag` → `my-team_my_dag`
2. Update connections: `postgres` → `my-team_postgres`
3. Update variables: `config` → `my-team_config`
4. Run validation: `make validate-dags`

### Adding a New Tenant

1. Add tenant to `KNOWN_TENANTS` (optional, for warnings)
2. Create directory: `dags/{tenant}/`
3. Start creating DAGs with tenant prefix
4. Use tenant utilities for connections/variables

## Troubleshooting

### "DAG ID must follow format: {tenant}_{name}"

**Solution**: Rename your DAG ID to include tenant prefix.

### "max_active_runs not set"

**Solution**: Add `max_active_runs=1` to DAG constructor.

### "Unknown tenant 'xyz'"

**Solution**: This is a warning, not an error. Add to `KNOWN_TENANTS` if you want to remove the warning.

## Future Enhancements

Potential improvements:
- Tenant-specific resource quotas
- Automatic tenant directory creation
- Tenant dashboard/analytics
- Connection/variable auto-prefixing
- Tenant-level access control

