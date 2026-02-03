"""
Common utilities for DAG validation scripts.

This module provides shared functionality to reduce code duplication.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

# Project root (set once)
PROJECT_ROOT = Path(__file__).parent.parent


def setup_airflow_environment(db_name: str = "validation") -> None:
    """Set up Airflow environment variables for validation."""
    os.environ.setdefault('AIRFLOW_HOME', str(PROJECT_ROOT))
    os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
    os.environ.setdefault(
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
        f'sqlite:////tmp/{db_name}_airflow.db'
    )
    os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'false')
    
    # Add project paths
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(PROJECT_ROOT / 'plugins'))


def find_dag_files(dags_dir: Optional[Path] = None) -> List[Path]:
    """
    Find all DAG files in the dags directory.
    
    Args:
        dags_dir: Directory to search (default: PROJECT_ROOT / 'dags')
        
    Returns:
        List of DAG file paths
    """
    if dags_dir is None:
        dags_dir = PROJECT_ROOT / 'dags'
    
    dag_files = list(dags_dir.rglob('*.py'))
    
    # Filter out non-DAG files
    return [
        f for f in dag_files
        if f.name != '__init__.py'
        and not f.name.startswith('test_')
    ]


def normalize_file_path(file_path: Path) -> Path:
    """
    Convert a file path to absolute path relative to project root.
    
    Args:
        file_path: Path to normalize
        
    Returns:
        Absolute path
    """
    if not file_path.is_absolute():
        return PROJECT_ROOT / file_path
    return file_path


def print_errors(errors: Dict[str, List[str]], title: str = "Errors") -> None:
    """Print errors in a consistent format."""
    if not errors:
        return
    
    print(f"❌ {title}:")
    for file_path, file_errors in errors.items():
        print(f"\n  {file_path}:")
        for error in file_errors:
            print(f"    - {error}")


def print_warnings(warnings: Dict[str, List[str]], title: str = "Warnings") -> None:
    """Print warnings in a consistent format."""
    if not warnings:
        return
    
    print(f"⚠️  {title}:")
    for file_path, file_warnings in warnings.items():
        print(f"\n  {file_path}:")
        for warning in file_warnings:
            print(f"    - {warning}")


def run_validation(
    check_function,
    files: Optional[List[Path]] = None,
    success_message: str = "✅ Validation passed",
    error_title: str = "Validation errors",
    warning_title: str = "Warnings",
) -> int:
    """
    Run a validation function on files and print results.
    
    Args:
        check_function: Function that takes a Path and returns (is_valid, errors, warnings)
        files: List of files to check (None = check all DAGs)
        success_message: Message to print on success
        error_title: Title for error output
        warning_title: Title for warning output
        
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    if files:
        # Check specific files
        all_valid = True
        all_errors = {}
        all_warnings = {}
        
        for dag_file in files:
            dag_file = normalize_file_path(dag_file)
            is_valid, errors, warnings = check_function(dag_file)
            
            if not is_valid:
                all_valid = False
            
            rel_path = str(dag_file.relative_to(PROJECT_ROOT))
            if errors:
                all_errors[rel_path] = errors
            if warnings:
                all_warnings[rel_path] = warnings
        
        print_warnings(all_warnings, warning_title)
        print_errors(all_errors, error_title)
        
        if not all_valid:
            return 1
        
        if all_warnings:
            print(f"{success_message} (with warnings)")
        else:
            print(success_message)
        return 0
    else:
        # Check all DAGs
        dag_files = find_dag_files()
        all_errors = {}
        all_warnings = {}
        
        for dag_file in dag_files:
            is_valid, errors, warnings = check_function(dag_file)
            
            rel_path = str(dag_file.relative_to(PROJECT_ROOT))
            if errors:
                all_errors[rel_path] = errors
            if warnings:
                all_warnings[rel_path] = warnings
        
        print_warnings(all_warnings, warning_title)
        print_errors(all_errors, error_title)
        
        if all_errors:
            return 1
        
        if all_warnings:
            print(f"{success_message} (with warnings)")
        else:
            print(success_message)
        return 0

