"""Tests for waiter plugin dag_loader."""

import pytest
from waiter.dag_loader import dags, DAGModule, DAGReference


class TestDAGModule:
    """Tests for DAGModule."""

    def test_dags_module_access(self):
        """Test accessing DAGs through dags module."""
        dag_ref = dags.test_dag
        assert isinstance(dag_ref, DAGReference)
        assert dag_ref.dag_id == 'test_dag'

    def test_dags_module_nested_access(self):
        """Test accessing tasks through dags module."""
        task_ref = dags.test_dag.test_task
        assert task_ref.dag_id == 'test_dag'
        assert task_ref.task_id == 'test_task'

    def test_dag_reference_repr(self):
        """Test DAGReference string representation."""
        dag_ref = DAGReference('test_dag')
        assert 'test_dag' in repr(dag_ref)

    def test_dag_reference_task_access(self):
        """Test accessing tasks from DAGReference."""
        dag_ref = DAGReference('test_dag')
        task_ref = dag_ref.test_task
        assert task_ref.dag_id == 'test_dag'
        assert task_ref.task_id == 'test_task'

