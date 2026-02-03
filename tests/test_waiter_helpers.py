"""Tests for waiter plugin helpers."""

import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

from airflow import DAG
from waiter.helpers import (
    TaskReference,
    wait_for_task,
    _is_cron_expression,
    _preset_to_timedelta,
    _cron_to_approximate_timedelta,
)
from waiter.operators import WaitForTaskOperator


class TestTaskReference:
    """Tests for TaskReference class."""

    def test_task_reference_creation(self):
        """Test creating a TaskReference."""
        ref = TaskReference(dag_id='test_dag', task_id='test_task')
        assert ref.dag_id == 'test_dag'
        assert ref.task_id == 'test_task'

    def test_task_reference_repr(self):
        """Test TaskReference string representation."""
        ref = TaskReference(dag_id='test_dag', task_id='test_task')
        assert 'test_dag' in repr(ref)
        assert 'test_task' in repr(ref)


class TestCronHelpers:
    """Tests for cron helper functions."""

    def test_is_cron_expression(self):
        """Test cron expression detection."""
        assert _is_cron_expression('0 2 * * *') is True
        assert _is_cron_expression('*/15 * * * *') is True
        assert _is_cron_expression('0 0 1 * *') is True
        assert _is_cron_expression('@daily') is False
        assert _is_cron_expression('@hourly') is False
        assert _is_cron_expression('timedelta(hours=1)') is False

    def test_preset_to_timedelta(self):
        """Test preset schedule conversion."""
        from waiter.helpers import _preset_to_timedelta
        
        assert _preset_to_timedelta('@hourly') == timedelta(hours=1)
        assert _preset_to_timedelta('@daily') == timedelta(days=1)
        assert _preset_to_timedelta('@weekly') == timedelta(weeks=1)
        assert _preset_to_timedelta('@once') is None
        assert _preset_to_timedelta('invalid') is None

    def test_cron_to_approximate_timedelta_basic(self):
        """Test basic cron to timedelta conversion."""
        # Hourly cron
        result = _cron_to_approximate_timedelta('0 * * * *')
        assert result == timedelta(hours=1)
        
        # Daily at midnight
        result = _cron_to_approximate_timedelta('0 0 * * *')
        assert result == timedelta(days=1)


class TestWaitForTask:
    """Tests for wait_for_task function."""

    def test_wait_for_task_with_task_reference(self):
        """Test wait_for_task with TaskReference."""
        task_ref = TaskReference(dag_id='test_dag', task_id='test_task')
        
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(task=task_ref)
            mock_operator.assert_called_once()
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['external_dag_id'] == 'test_dag'
            assert call_kwargs['external_task_id'] == 'test_task'

    def test_wait_for_task_with_string(self):
        """Test wait_for_task with string format."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(task='test_dag.test_task')
            mock_operator.assert_called_once()
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['external_dag_id'] == 'test_dag'
            assert call_kwargs['external_task_id'] == 'test_task'

    def test_wait_for_task_with_string_invalid(self):
        """Test wait_for_task with invalid string format."""
        with pytest.raises(ValueError, match="format 'dag_id.task_id'"):
            wait_for_task(task='invalid_format')

    def test_wait_for_task_with_operator(self):
        """Test wait_for_task with BaseOperator."""
        from airflow.operators.bash import BashOperator
        
        # Create a DAG and operator directly
        dag = DAG('test_dag', start_date=datetime(2024, 1, 1), schedule=timedelta(days=1))
        operator = BashOperator(
            task_id='test_task',
            bash_command='echo "test"',
            dag=dag
        )
        
        result = wait_for_task(task=operator)
        assert result is not None
        assert isinstance(result, WaitForTaskOperator)
        assert result.external_dag_id == 'test_dag'
        assert result.external_task_id == 'test_task'

    def test_wait_for_task_auto_generate_task_id(self):
        """Test automatic task_id generation."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(task='test_dag.test_task')
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['task_id'] == 'wait_for_test_dag_test_task'

    def test_wait_for_task_custom_task_id(self):
        """Test custom task_id."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(task='test_dag.test_task', task_id='custom_wait')
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['task_id'] == 'custom_wait'

    def test_wait_for_task_mode_validation(self):
        """Test mode parameter validation."""
        with pytest.raises(ValueError, match="mode must be 'poke' or 'reschedule'"):
            wait_for_task(task='test_dag.test_task', mode='invalid')

    def test_wait_for_task_retry_parameters(self):
        """Test retry parameters are passed correctly."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(
                task='test_dag.test_task',
                retries=3,
                retry_delay=timedelta(minutes=10),
            )
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['retries'] == 3
            assert call_kwargs['retry_delay'] == timedelta(minutes=10)

    def test_wait_for_task_mode_parameters(self):
        """Test mode parameter is passed correctly."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(task='test_dag.test_task', mode='reschedule')
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['mode'] == 'reschedule'


class TestScheduleReconciliation:
    """Tests for schedule reconciliation."""

    @patch('waiter.helpers.DagBag')
    def test_schedule_reconciliation_same_schedules(self, mock_dagbag):
        """Test reconciliation with same schedules."""
        current_dag = DAG('current', schedule=timedelta(days=1))
        external_dag = DAG('external', schedule=timedelta(days=1))
        
        mock_bag = MagicMock()
        mock_bag.get_dag.return_value = external_dag
        mock_dagbag.return_value = mock_bag
        
        with patch('waiter.helpers._calculate_execution_delta') as mock_calc:
            mock_calc.return_value = (timedelta(0), None)
            wait_for_task(
                task='external.test_task',
                current_dag=current_dag,
                auto_reconcile_schedules=True,
            )
            mock_calc.assert_called_once()

    @patch('waiter.helpers.DagBag')
    def test_schedule_reconciliation_disabled(self, mock_dagbag):
        """Test that reconciliation can be disabled."""
        with patch('waiter.helpers.WaitForTaskOperator') as mock_operator:
            wait_for_task(
                task='external.test_task',
                auto_reconcile_schedules=False,
                execution_delta=timedelta(hours=1),
            )
            call_kwargs = mock_operator.call_args[1]
            assert call_kwargs['execution_delta'] == timedelta(hours=1)

