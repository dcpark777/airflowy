"""Tests for waiter plugin operators."""

import os
import pytest
from datetime import timedelta
from unittest.mock import Mock, patch
from pathlib import Path

# Set Airflow environment before importing
os.environ.setdefault('AIRFLOW_HOME', str(Path(__file__).parent.parent))
os.environ.setdefault('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')
os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

from airflow.utils.state import TaskInstanceState
from waiter.operators import WaitForTaskOperator


class TestWaitForTaskOperator:
    """Tests for WaitForTaskOperator."""

    def test_operator_initialization(self):
        """Test basic operator initialization."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
        )
        assert operator.external_dag_id == 'external_dag'
        assert operator.external_task_id == 'external_task'
        assert operator.task_id == 'test_wait'

    def test_operator_default_allowed_states(self):
        """Test default allowed states."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
        )
        assert TaskInstanceState.SUCCESS in operator.allowed_states

    def test_operator_custom_allowed_states(self):
        """Test custom allowed states."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            allowed_states=[TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED],
        )
        assert len(operator.allowed_states) == 2
        assert TaskInstanceState.SUCCESS in operator.allowed_states
        assert TaskInstanceState.SKIPPED in operator.allowed_states

    def test_operator_mode_parameter(self):
        """Test mode parameter."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            mode='reschedule',
        )
        assert operator.mode == 'reschedule'

    def test_operator_retry_parameters(self):
        """Test retry parameters."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            retries=3,
            retry_delay=timedelta(minutes=10),
        )
        assert operator.retries == 3
        assert operator.retry_delay == timedelta(minutes=10)

    def test_operator_poke_interval(self):
        """Test poke_interval parameter."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            poke_interval=300,
        )
        assert operator.poke_interval == 300

    def test_operator_timeout(self):
        """Test timeout parameter."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            timeout=3600,
        )
        assert operator.timeout == 3600

    def test_operator_execution_delta(self):
        """Test execution_delta parameter."""
        operator = WaitForTaskOperator(
            task_id='test_wait',
            external_dag_id='external_dag',
            external_task_id='external_task',
            execution_delta=timedelta(hours=1),
        )
        assert operator.execution_delta == timedelta(hours=1)

