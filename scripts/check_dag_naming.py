#!/usr/bin/env python3
"""
DAG Naming Convention Checker

Validates that DAGs follow the tenant naming convention: {tenant}_{dag_name}

Usage:
    python scripts/check_dag_naming.py
    python scripts/check_dag_naming.py dags/my_team/my_dag.py
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Setup paths before imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'plugins'))

from scripts.common import (
    setup_airflow_environment,
    run_validation,
)
from scripts.dag_parser import get_dag_info, extract_dag_ids_from_ast
from plugins.tenants.helpers import get_tenant_from_dag_id, validate_tenant, KNOWN_TENANTS


def check_naming_convention(dag_id: str) -> Tuple[bool, str, bool]:
    """
    Check if DAG ID follows naming convention: {tenant}_{name}.
    
    Returns:
        Tuple of (is_valid, message, is_warning)
    """
    if not dag_id:
        return False, "DAG ID is empty", False
    
    # Check format: should have at least one underscore
    if '_' not in dag_id:
        return False, f"DAG ID '{dag_id}' must follow format: {{tenant}}_{{name}}", False
    
    parts = dag_id.split('_', 1)
    if len(parts) != 2:
        return False, f"DAG ID '{dag_id}' must follow format: {{tenant}}_{{name}}", False
    
    tenant, name = parts
    
    # Check tenant is not empty
    if not tenant:
        return False, f"DAG ID '{dag_id}' has empty tenant prefix", False
    
    # Check name is not empty
    if not name:
        return False, f"DAG ID '{dag_id}' has empty name after tenant prefix", False
    
    # Validate tenant format
    if not validate_tenant(tenant):
        return False, (
            f"DAG ID '{dag_id}' has invalid tenant format "
            "(use lowercase, alphanumeric, hyphens)"
        ), False
    
    # Check name format (should be lowercase, alphanumeric with underscores)
    import re
    if not re.match(r'^[a-z0-9_]+$', name):
        return False, (
            f"DAG ID '{dag_id}' has invalid name format "
            "(use lowercase, alphanumeric, underscores)"
        ), False
    
    # Warning if tenant not in known list (not a failure)
    if tenant not in KNOWN_TENANTS:
        return True, f"Unknown tenant '{tenant}' (valid but not in known list)", True
    
    return True, "Valid", False


def check_dag_file(dag_file: Path) -> Tuple[bool, List[str], List[str]]:
    """Check naming conventions for a single DAG file."""
    errors = []
    warnings = []
    
    # Try to extract DAG IDs from file
    dag_ids = extract_dag_ids_from_ast(dag_file)
    
    # Also try loading via DagBag
    dag_info = get_dag_info(dag_file)
    if dag_info.get('dag_id') and dag_info['dag_id'] not in dag_ids:
        dag_ids.append(dag_info['dag_id'])
    
    # Check each DAG ID
    for dag_id in dag_ids:
        is_valid, message, is_warning = check_naming_convention(dag_id)
        if not is_valid:
            errors.append(f"{dag_id}: {message}")
        elif is_warning:
            warnings.append(f"{dag_id}: {message}")
    
    # If no DAG IDs found, that's also an error
    if not dag_ids:
        errors.append("No DAG ID found in file")
    
    return len(errors) == 0, errors, warnings


def main():
    """Main function."""
    setup_airflow_environment("check_naming")
    
    # Get files from command line or check all
    files = None
    if len(sys.argv) > 1:
        files = [Path(f) for f in sys.argv[1:]]
    
    exit_code = run_validation(
        check_function=check_dag_file,
        files=files,
        success_message="✅ All DAGs follow naming convention",
        error_title="DAG naming convention violations",
        warning_title="Naming warnings",
    )
    
    if exit_code != 0:
        print("\nDAG IDs must follow format: {tenant}_{name}")
        print("Example: data-engineering_daily_etl")
    
    return exit_code


if __name__ == '__main__':
    exit(main())
