#!/usr/bin/env python3
"""
DAG Validation Script

Validates DAG files for syntax errors and import issues.
Can be used as a pre-commit hook or standalone script.

Usage:
    python scripts/validate_dags.py
    python scripts/validate_dags.py dags/my_team/my_dag.py
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Import after path setup
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.common import (
    setup_airflow_environment,
    find_dag_files,
    normalize_file_path,
    run_validation,
)
from airflow.models import DagBag


def validate_dag_file(dag_file: Path) -> Tuple[bool, List[str], List[str]]:
    """Validate a single DAG file for syntax and import errors."""
    errors = []
    
    if not dag_file.exists():
        return False, [f"DAG file not found: {dag_file}"], []
    
    # Load DAG
    try:
        dag_bag = DagBag(
            dag_folder=str(dag_file.parent),
            include_examples=False,
        )
    except Exception as e:
        return False, [f"Failed to load DAG bag: {e}"], []
    
    # Check for import errors
    if dag_bag.import_errors:
        for file_path, error in dag_bag.import_errors.items():
            if str(dag_file) in file_path or dag_file.name in file_path:
                errors.append(f"Import error: {error}")
    
    # Check if DAG was loaded
    dag_id = None
    for loaded_dag_id, dag in dag_bag.dags.items():
        if hasattr(dag, 'fileloc') and str(dag_file) in dag.fileloc:
            dag_id = loaded_dag_id
            break
    
    if not dag_id and not errors:
        errors.append("DAG not found - check for DAG definition")
    
    return len(errors) == 0, errors, []


def main():
    """Main validation function."""
    setup_airflow_environment("validate")
    
    # Get files from command line or check all
    files = None
    if len(sys.argv) > 1:
        files = [Path(f) for f in sys.argv[1:]]
    
    return run_validation(
        check_function=validate_dag_file,
        files=files,
        success_message="✅ All DAGs are valid",
        error_title="DAG validation errors",
    )


if __name__ == '__main__':
    exit(main())
