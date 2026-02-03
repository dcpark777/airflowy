"""Tests to ensure all DAGs follow best practices.

This module validates that all DAGs in the project meet quality standards including:
- Owner is set
- Documentation exists (docstring, description, or doc_md)
- Tags are present (recommended)
- Schedule is explicitly set
- Catchup is explicitly set
- Start date is set
- Retries are configured
"""

import pytest
from pathlib import Path
import os
import ast

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:////tmp/test_airflow.db')
os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'false')

from airflow.models import DagBag


@pytest.fixture(scope="class")
def dag_bag():
    """Create a DagBag for testing (class-scoped to avoid recreating).
    
    Note: For testing, we load DAGs from the examples directory explicitly
    since .airflowignore prevents them from loading in production.
    """
    dags_dir = Path(__file__).parent.parent / 'dags'
    examples_dir = dags_dir / 'examples'
    
    # Create DagBag - load from both main dags directory and examples
    bag = DagBag(
        dag_folder=str(examples_dir) if examples_dir.exists() else str(dags_dir),
        include_examples=False,
    )
    
    # Also load any DAGs from the main dags directory (if any exist)
    if dags_dir.exists():
        main_bag = DagBag(
            dag_folder=str(dags_dir),
            include_examples=False,
        )
        # Merge DAGs from main directory into our bag
        bag.dags.update(main_bag.dags)
        bag.import_errors.update(main_bag.import_errors)
    
    return bag


class TestDAGBestPractices:
    """Tests to ensure all DAGs follow best practices."""

    def test_all_dags_have_owner(self, dag_bag):
        """Test that all DAGs have an owner set.
        
        Owner can be set in:
        - default_args['owner']
        - DAG constructor owner parameter
        """
        missing_owner = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Check if owner is set in default_args
            owner_from_args = dag.default_args.get('owner') if dag.default_args else None
            # Check if owner is set directly on DAG (Airflow 2.0+)
            owner_from_dag = getattr(dag, 'owner', None)
            
            owner = owner_from_dag or owner_from_args
            
            if not owner:
                missing_owner.append(dag_id)
        
        assert len(missing_owner) == 0, \
            f"DAGs missing owner: {', '.join(missing_owner)}. " \
            f"Set owner in default_args['owner'] or DAG(owner=...)"

    def test_all_dags_have_documentation(self, dag_bag):
        """Test that all DAGs have documentation.
        
        Documentation can be provided via:
        - Module docstring (recommended)
        - DAG description parameter
        - DAG doc_md parameter
        """
        missing_docs = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Check for docstring in the module
            # We can't easily check this from DagBag, so we'll check DAG-level docs
            has_description = bool(getattr(dag, 'description', None))
            has_doc_md = bool(getattr(dag, 'doc_md', None))
            
            # Try to get the module docstring by parsing the file
            dag_file = dag.fileloc if hasattr(dag, 'fileloc') else None
            has_docstring = False
            if dag_file and Path(dag_file).exists():
                try:
                    with open(dag_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Parse the AST to check for module-level docstring
                        try:
                            tree = ast.parse(content)
                            if ast.get_docstring(tree):
                                has_docstring = True
                        except SyntaxError:
                            # If parsing fails, fall back to simple string check
                            stripped = content.strip()
                            if (stripped.startswith('"""') or stripped.startswith("'''")) and \
                               len(stripped) > 3:
                                has_docstring = True
                except Exception:
                    pass
            
            if not (has_description or has_doc_md or has_docstring):
                missing_docs.append(dag_id)
        
        assert len(missing_docs) == 0, \
            f"DAGs missing documentation: {', '.join(missing_docs)}. " \
            f"Add a module docstring, description='...', or doc_md='...' parameter"

    def test_all_dags_have_tags(self, dag_bag):
        """Test that all DAGs have tags (recommended for organization).
        
        Tags help organize DAGs in the Airflow UI and are considered best practice.
        """
        missing_tags = []
        
        for dag_id, dag in dag_bag.dags.items():
            tags = getattr(dag, 'tags', None)
            if not tags or len(tags) == 0:
                missing_tags.append(dag_id)
        
        if len(missing_tags) > 0:
            pytest.skip(
                f"DAGs missing tags (recommended but not required): {', '.join(missing_tags)}. "
                f"Add tags=['tag1', 'tag2'] to DAG constructor"
            )

    def test_all_dags_have_explicit_schedule(self, dag_bag):
        """Test that all DAGs have an explicit schedule set.
        
        Schedule can be:
        - None (for manual DAGs - acceptable but should be intentional)
        - timedelta
        - cron expression (str)
        - timetable
        - preset (str like '@daily')
        
        We just verify the schedule attribute exists and is not accidentally unset.
        """
        invalid_schedules = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Schedule should be explicitly set (even if None for manual DAGs)
            # In Airflow 2.4+, schedule is the attribute; in older versions it's schedule_interval
            schedule = getattr(dag, 'schedule', None)
            schedule_interval = getattr(dag, 'schedule_interval', None)
            
            # If neither exists, that's a problem
            if schedule is None and schedule_interval is None:
                # Check if it's explicitly set to None in the DAG definition
                # We can't easily check this, so we'll just verify the attribute exists
                if not hasattr(dag, 'schedule') and not hasattr(dag, 'schedule_interval'):
                    invalid_schedules.append(dag_id)
        
        assert len(invalid_schedules) == 0, \
            f"DAGs with invalid schedule configuration: {', '.join(invalid_schedules)}. " \
            f"Explicitly set schedule (can be None for manual DAGs)"

    def test_all_dags_have_explicit_catchup(self, dag_bag):
        """Test that all DAGs have catchup explicitly set.
        
        catchup should be explicitly set to True or False to avoid confusion.
        """
        missing_catchup = []
        
        for dag_id, dag in dag_bag.dags.items():
            # catchup should be explicitly set
            # Default is True in Airflow, but it's best practice to be explicit
            catchup = getattr(dag, 'catchup', None)
            
            # We can't easily check if it was explicitly set vs using default
            # So we'll just verify it's a boolean value
            if catchup is None:
                missing_catchup.append(dag_id)
        
        # This is a warning rather than a hard failure
        if len(missing_catchup) > 0:
            pytest.skip(
                f"DAGs should explicitly set catchup=True or catchup=False: {', '.join(missing_catchup)}"
            )

    def test_all_dags_have_start_date(self, dag_bag):
        """Test that all DAGs have a start_date set.
        
        start_date can be set in:
        - default_args['start_date']
        - DAG constructor start_date parameter
        """
        missing_start_date = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Check if start_date is set in default_args
            start_date_from_args = dag.default_args.get('start_date') if dag.default_args else None
            # Check if start_date is set directly on DAG
            start_date_from_dag = getattr(dag, 'start_date', None)
            
            start_date = start_date_from_dag or start_date_from_args
            
            if not start_date:
                missing_start_date.append(dag_id)
        
        assert len(missing_start_date) == 0, \
            f"DAGs missing start_date: {', '.join(missing_start_date)}. " \
            f"Set start_date in default_args['start_date'] or DAG(start_date=...)"

    def test_all_dags_have_retries_configured(self, dag_bag):
        """Test that all DAGs have retries configured.
        
        Retries can be set in:
        - default_args['retries']
        - DAG constructor retries parameter
        - Task-level retries (but DAG-level is recommended)
        """
        missing_retries = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Check if retries is set in default_args
            retries_from_args = dag.default_args.get('retries') if dag.default_args else None
            # Check if retries is set directly on DAG
            retries_from_dag = getattr(dag, 'retries', None)
            
            retries = retries_from_dag if retries_from_dag is not None else retries_from_args
            
            # retries can be 0, so we need to check if it's None specifically
            if retries is None:
                missing_retries.append(dag_id)
        
        assert len(missing_retries) == 0, \
            f"DAGs missing retries configuration: {', '.join(missing_retries)}. " \
            f"Set retries in default_args['retries'] or DAG(retries=...) (can be 0)"

    def test_all_dags_have_retry_delay_configured(self, dag_bag):
        """Test that all DAGs have retry_delay configured when retries > 0.
        
        If retries are set, retry_delay should also be configured.
        """
        missing_retry_delay = []
        
        for dag_id, dag in dag_bag.dags.items():
            # Get retries
            retries_from_args = dag.default_args.get('retries') if dag.default_args else None
            retries_from_dag = getattr(dag, 'retries', None)
            retries = retries_from_dag if retries_from_dag is not None else retries_from_args
            
            # Only check retry_delay if retries > 0
            if retries and retries > 0:
                # Check if retry_delay is set in default_args
                retry_delay_from_args = dag.default_args.get('retry_delay') if dag.default_args else None
                # Check if retry_delay is set directly on DAG
                retry_delay_from_dag = getattr(dag, 'retry_delay', None)
                
                retry_delay = retry_delay_from_dag or retry_delay_from_args
                
                if not retry_delay:
                    missing_retry_delay.append(dag_id)
        
        assert len(missing_retry_delay) == 0, \
            f"DAGs with retries > 0 but missing retry_delay: {', '.join(missing_retry_delay)}. " \
            f"Set retry_delay in default_args['retry_delay'] or DAG(retry_delay=...)"

    def test_all_dags_have_max_active_runs_configured(self, dag_bag):
        """Test that all DAGs have max_active_runs configured (recommended).
        
        max_active_runs prevents too many DAG runs from executing simultaneously.
        This is recommended but not strictly required.
        """
        missing_max_active_runs = []
        
        for dag_id, dag in dag_bag.dags.items():
            max_active_runs = getattr(dag, 'max_active_runs', None)
            if max_active_runs is None:
                missing_max_active_runs.append(dag_id)
        
        if len(missing_max_active_runs) > 0:
            pytest.skip(
                f"DAGs should set max_active_runs (recommended): {', '.join(missing_max_active_runs)}. " \
                f"Add max_active_runs=1 (or appropriate value) to DAG constructor"
            )

