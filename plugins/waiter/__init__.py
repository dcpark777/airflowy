"""Waiter plugin for waiting on tasks from other DAGs."""

from waiter.operators import WaitForTaskOperator
from waiter.helpers import wait_for_task, TaskReference
from waiter.dag_loader import dags

__all__ = ['WaitForTaskOperator', 'wait_for_task', 'TaskReference', 'dags']

