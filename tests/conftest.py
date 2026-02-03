"""Pytest configuration and fixtures."""

import sys
import os
from pathlib import Path

# Add project root and plugins to path
project_root = Path(__file__).parent.parent
plugins_dir = project_root / 'plugins'

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(plugins_dir))

# Set AIRFLOW_HOME to avoid Airflow initialization issues in tests
os.environ.setdefault('AIRFLOW_HOME', str(project_root))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

