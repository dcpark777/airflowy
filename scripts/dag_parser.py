"""
DAG file parsing utilities.

Provides functions to extract DAG information from Python files using AST parsing
and Airflow DagBag loading.
"""

import ast
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Setup paths before Airflow imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'plugins'))

from airflow.models import DagBag


def extract_ast_value(node: ast.AST) -> Any:
    """
    Extract a constant value from an AST node.
    
    Supports both Python 3.8+ (ast.Constant) and older versions.
    """
    if isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.Str):  # Python < 3.8
        return node.s
    elif isinstance(node, ast.Num):  # Python < 3.8
        return node.n
    return None


def extract_dag_config_from_ast(dag_file: Path) -> Dict[str, Any]:
    """
    Extract DAG configuration from a Python file using AST parsing.
    
    Args:
        dag_file: Path to DAG file
        
    Returns:
        Dictionary with extracted config (dag_id, max_active_runs, max_active_tasks)
    """
    config = {
        'dag_id': None,
        'max_active_runs': None,
        'max_active_tasks': None,
    }
    
    try:
        with open(dag_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Look for DAG() constructor calls
                if isinstance(node.func, ast.Name) and node.func.id == 'DAG':
                    # Extract keyword arguments
                    for keyword in node.keywords:
                        if keyword.arg == 'dag_id':
                            config['dag_id'] = extract_ast_value(keyword.value)
                        elif keyword.arg == 'max_active_runs':
                            config['max_active_runs'] = extract_ast_value(keyword.value)
                        elif keyword.arg == 'max_active_tasks':
                            config['max_active_tasks'] = extract_ast_value(keyword.value)
    except Exception:
        # AST parsing failed, will fall back to DagBag
        pass
    
    return config


def extract_dag_ids_from_ast(dag_file: Path) -> List[str]:
    """
    Extract DAG IDs from a Python file using AST parsing.
    
    Args:
        dag_file: Path to DAG file
        
    Returns:
        List of DAG IDs found in the file
    """
    dag_ids = []
    
    try:
        with open(dag_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == 'DAG':
                    for keyword in node.keywords:
                        if keyword.arg == 'dag_id':
                            dag_id = extract_ast_value(keyword.value)
                            if dag_id:
                                dag_ids.append(dag_id)
    except Exception:
        pass
    
    return dag_ids


def load_dag_from_dagbag(dag_file: Path) -> Optional[Dict[str, Any]]:
    """
    Load DAG information using Airflow DagBag.
    
    Args:
        dag_file: Path to DAG file
        
    Returns:
        Dictionary with dag_id and DAG object, or None if not found
    """
    try:
        dag_bag = DagBag(
            dag_folder=str(dag_file.parent),
            include_examples=False,
        )
        
        # Find DAG that matches this file
        for dag_id, dag in dag_bag.dags.items():
            if hasattr(dag, 'fileloc') and str(dag_file) in dag.fileloc:
                return {
                    'dag_id': dag_id,
                    'dag': dag,
                }
    except Exception:
        pass
    
    return None


def get_dag_info(dag_file: Path, use_dagbag: bool = True) -> Dict[str, Any]:
    """
    Get DAG information from a file using both AST and DagBag.
    
    Args:
        dag_file: Path to DAG file
        use_dagbag: Whether to also try loading via DagBag
        
    Returns:
        Dictionary with dag_id, max_active_runs, max_active_tasks, and dag object
    """
    # Start with AST parsing
    config = extract_dag_config_from_ast(dag_file)
    
    # Enhance with DagBag if available
    if use_dagbag:
        dagbag_info = load_dag_from_dagbag(dag_file)
        if dagbag_info:
            if not config['dag_id']:
                config['dag_id'] = dagbag_info['dag_id']
            
            dag = dagbag_info['dag']
            if config['max_active_runs'] is None:
                config['max_active_runs'] = getattr(dag, 'max_active_runs', None)
            if config['max_active_tasks'] is None:
                config['max_active_tasks'] = getattr(dag, 'max_active_tasks', None)
            
            config['dag'] = dag
    
    return config

