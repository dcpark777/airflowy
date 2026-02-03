#!/usr/bin/env python3
"""
DAG Resource Limits Checker

Validates that DAGs have proper resource limits configured to prevent resource exhaustion.

Usage:
    python scripts/check_dag_resources.py
    python scripts/check_dag_resources.py dags/my_team/my_dag.py
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
from scripts.dag_parser import get_dag_info


# Resource limit constants
DEFAULT_MAX_ACTIVE_RUNS = 1
DEFAULT_MAX_ACTIVE_TASKS = 3
MAX_ALLOWED_ACTIVE_RUNS = 5
MAX_ALLOWED_ACTIVE_TASKS = 10


def check_dag_resources(dag_file: Path) -> Tuple[bool, List[str], List[str]]:
    """Check resource limits for a single DAG file."""
    errors = []
    warnings = []
    
    # Get DAG info
    config = get_dag_info(dag_file)
    dag_id = config.get('dag_id') or dag_file.stem
    
    max_active_runs = config.get('max_active_runs')
    max_active_tasks = config.get('max_active_tasks')
    
    # Check max_active_runs
    if max_active_runs is None:
        warnings.append(
            f"{dag_id}: max_active_runs not set (recommended: {DEFAULT_MAX_ACTIVE_RUNS})"
        )
    elif max_active_runs > MAX_ALLOWED_ACTIVE_RUNS:
        errors.append(
            f"{dag_id}: max_active_runs={max_active_runs} exceeds maximum "
            f"allowed ({MAX_ALLOWED_ACTIVE_RUNS})"
        )
    elif max_active_runs > DEFAULT_MAX_ACTIVE_RUNS:
        warnings.append(
            f"{dag_id}: max_active_runs={max_active_runs} is higher than "
            f"recommended ({DEFAULT_MAX_ACTIVE_RUNS})"
        )
    
    # Check max_active_tasks
    if max_active_tasks is None:
        warnings.append(
            f"{dag_id}: max_active_tasks not set (recommended: {DEFAULT_MAX_ACTIVE_TASKS})"
        )
    elif max_active_tasks > MAX_ALLOWED_ACTIVE_TASKS:
        errors.append(
            f"{dag_id}: max_active_tasks={max_active_tasks} exceeds maximum "
            f"allowed ({MAX_ALLOWED_ACTIVE_TASKS})"
        )
    elif max_active_tasks > DEFAULT_MAX_ACTIVE_TASKS:
        warnings.append(
            f"{dag_id}: max_active_tasks={max_active_tasks} is higher than "
            f"recommended ({DEFAULT_MAX_ACTIVE_TASKS})"
        )
    
    return len(errors) == 0, errors, warnings


def main():
    """Main function."""
    setup_airflow_environment("check_resources")
    
    # Get files from command line or check all
    files = None
    if len(sys.argv) > 1:
        files = [Path(f) for f in sys.argv[1:]]
    
    exit_code = run_validation(
        check_function=check_dag_resources,
        files=files,
        success_message="✅ All DAGs have proper resource limits",
        error_title="Resource limit violations",
        warning_title="Resource limit warnings",
    )
    
    if exit_code != 0:
        print(f"\nRecommended limits:")
        print(f"  - max_active_runs: {DEFAULT_MAX_ACTIVE_RUNS} (max: {MAX_ALLOWED_ACTIVE_RUNS})")
        print(f"  - max_active_tasks: {DEFAULT_MAX_ACTIVE_TASKS} (max: {MAX_ALLOWED_ACTIVE_TASKS})")
    
    return exit_code


if __name__ == '__main__':
    exit(main())
