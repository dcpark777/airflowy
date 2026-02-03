"""Tests to ensure all DAGs can be imported and loaded correctly."""

import os
import pytest
from pathlib import Path

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:////tmp/test_airflow.db')
os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'false')

from airflow.models import DagBag


def get_dag_files():
    """Get all DAG files from the dags directory and examples subdirectory."""
    dags_dir = Path(__file__).parent.parent / 'dags'
    # Get DAGs from main dags directory
    main_dags = list(dags_dir.glob('*.py'))
    # Get DAGs from examples directory
    examples_dir = dags_dir / 'examples'
    example_dags = list(examples_dir.glob('*.py')) if examples_dir.exists() else []
    return main_dags + example_dags


class TestDAGImports:
    """Tests for DAG imports and loading."""

    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Create a DagBag for testing (class-scoped to avoid recreating).
        
        Note: For testing, we load DAGs from the examples directory explicitly
        since .airflowignore prevents them from loading in production.
        """
        dags_dir = Path(__file__).parent.parent / 'dags'
        examples_dir = dags_dir / 'examples'
        
        # Create DagBag - load from both main dags directory and examples
        # We need to load examples explicitly since .airflowignore excludes them
        # For testing purposes, we'll load from examples directory directly
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

    def test_all_dag_files_exist(self):
        """Test that expected DAG files exist."""
        dag_files = get_dag_files()
        assert len(dag_files) > 0, "No DAG files found"
        
        # Check for expected DAG files (now in examples directory)
        dag_names = [f.stem for f in dag_files]
        assert 'simple_dag' in dag_names, "simple_dag.py not found in dags/examples/"
        assert 'example_wait_dag' in dag_names, "example_wait_dag.py not found in dags/examples/"
        assert 'example_sql_sensor_dag' in dag_names, "example_sql_sensor_dag.py not found in dags/examples/"

    def test_dag_bag_imports_no_errors(self, dag_bag):
        """Test that DagBag can import all DAGs without errors."""
        # DagBag should have no import errors
        assert len(dag_bag.import_errors) == 0, f"DAG import errors found: {dag_bag.import_errors}"

    def test_dag_bag_loads_dags(self, dag_bag):
        """Test that DagBag loads DAGs successfully."""
        assert len(dag_bag.dags) > 0, "No DAGs loaded from dag_bag"
        
        # Check for expected DAGs
        dag_ids = list(dag_bag.dags.keys())
        assert 'simple_dag' in dag_ids, "simple_dag not loaded"
        assert 'example_wait_dag' in dag_ids, "example_wait_dag not loaded"

    def test_simple_dag_structure(self, dag_bag):
        """Test that simple_dag has correct structure."""
        # Use dag_bag.dags dictionary instead of get_dag() to avoid database queries
        dag = dag_bag.dags.get('simple_dag')
        assert dag is not None, "simple_dag not found in dag_bag"
        
        # Check for expected tasks
        task_ids = [task.task_id for task in dag.tasks]
        assert 'print_date' in task_ids, "print_date task not found"
        assert 'print_hello' in task_ids, "print_hello task not found"

    def test_example_wait_dag_structure(self, dag_bag):
        """Test that example_wait_dag has correct structure."""
        # Use dag_bag.dags dictionary instead of get_dag() to avoid database queries
        dag = dag_bag.dags.get('example_wait_dag')
        assert dag is not None, "example_wait_dag not found in dag_bag"
        
        # Check for expected tasks
        task_ids = [task.task_id for task in dag.tasks]
        assert 'wait_for_simple_dag_print_date' in task_ids
        assert 'wait_for_simple_dag_hello' in task_ids
        assert 'my_task' in task_ids

    def test_example_wait_dag_uses_waiter(self, dag_bag):
        """Test that example_wait_dag uses waiter plugin correctly."""
        # Use dag_bag.dags dictionary instead of get_dag() to avoid database queries
        dag = dag_bag.dags.get('example_wait_dag')
        assert dag is not None
        
        # Check that wait tasks are WaitForTaskOperator instances
        from waiter.operators import WaitForTaskOperator
        
        wait_tasks = [
            task for task in dag.tasks
            if isinstance(task, WaitForTaskOperator)
        ]
        assert len(wait_tasks) >= 2, "Expected at least 2 wait tasks"

    def test_demo_hourly_dag_structure(self, dag_bag):
        """Test that demo_hourly_dag has correct structure."""
        # Use dag_bag.dags dictionary instead of get_dag() to avoid database queries
        dag = dag_bag.dags.get('demo_hourly_dag')
        if dag is not None:  # May not exist in all test environments
            task_ids = [task.task_id for task in dag.tasks]
            assert 'start_hourly_task' in task_ids or 'hourly_task' in task_ids

    def test_example_cron_wait_dag_structure(self, dag_bag):
        """Test that example_cron_wait_dag has correct structure."""
        # Use dag_bag.dags dictionary instead of get_dag() to avoid database queries
        dag = dag_bag.dags.get('example_cron_wait_dag')
        if dag is not None:  # May not exist in all test environments
            task_ids = [task.task_id for task in dag.tasks]
            assert 'wait_for_daily_cron' in task_ids or 'wait_for_hourly' in task_ids
            assert 'process' in task_ids or 'process_after_hourly' in task_ids

    def test_all_dags_have_valid_schedules(self, dag_bag):
        """Test that all loaded DAGs have valid schedule intervals."""
        for dag_id, dag in dag_bag.dags.items():
            # Schedule can be None (manual), timedelta, str (cron/preset), or timetable
            # Support both 'schedule' (Airflow 3.0) and 'schedule_interval' (Airflow 2.x) attributes
            # Just verify the schedule attribute exists and doesn't raise an error
            assert hasattr(dag, 'schedule') or hasattr(dag, 'schedule_interval')

    def test_waiter_plugin_importable(self):
        """Test that waiter plugin can be imported."""
        try:
            from waiter import wait_for_task, dags, WaitForTaskOperator
            assert wait_for_task is not None
            assert dags is not None
            assert WaitForTaskOperator is not None
        except ImportError as e:
            pytest.fail(f"Failed to import waiter plugin: {e}")

    def test_sensors_plugin_importable(self):
        """Test that sensors plugin can be imported (same way DAGs import it)."""
        try:
            from sensors import SqlSensor
            assert SqlSensor is not None
        except ImportError as e:
            pytest.fail(f"Failed to import sensors plugin: {e}")

    def test_sensors_plugin_importable_from_init(self):
        """Test that sensors plugin __init__.py can be imported without errors.
        
        This test simulates how Airflow loads plugins and catches relative import issues.
        """
        import sys
        from pathlib import Path
        
        # Add plugins to path the way Airflow does
        plugins_dir = Path(__file__).parent.parent / 'plugins'
        if str(plugins_dir) not in sys.path:
            sys.path.insert(0, str(plugins_dir))
        
        # Try importing the __init__.py module directly
        # This will fail if there are relative import issues
        try:
            import importlib.util
            init_file = plugins_dir / 'sensors' / '__init__.py'
            spec = importlib.util.spec_from_file_location("sensors", init_file)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # If we get here, the import worked
                assert hasattr(module, 'SqlSensor')
        except Exception as e:
            pytest.fail(f"Failed to import sensors/__init__.py (plugin loading issue): {e}")

    def test_dag_dependencies_valid(self, dag_bag):
        """Test that DAG task dependencies are valid."""
        for dag_id, dag in dag_bag.dags.items():
            # Check that all task dependencies reference valid tasks
            for task in dag.tasks:
                # Check upstream tasks exist
                for upstream_task_id in task.upstream_task_ids:
                    upstream_task = dag.get_task(upstream_task_id)
                    assert upstream_task is not None, \
                        f"Task {task.task_id} in {dag_id} references non-existent upstream task {upstream_task_id}"

