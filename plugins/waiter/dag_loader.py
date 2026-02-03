"""Helper module for importing tasks from other DAGs.

This module provides a way to reference tasks from other DAGs in a convenient way.

Example:
    ```python
    from waiter import wait_for_task, dags

    # Reference a task from another DAG
    my_task = dags.my_dag.my_task_id
    
    # Use it with wait_for_task
    wait_task = wait_for_task(task=my_task)
    ```
"""

from waiter.helpers import TaskReference


class DAGModule:
    """Module-like object for accessing DAGs and their tasks."""

    def __getattr__(self, dag_id: str):
        return DAGReference(dag_id)


class DAGReference:
    """Reference to a DAG that allows accessing its tasks."""

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    def __getattr__(self, task_id: str) -> TaskReference:
        """Return a TaskReference for the specified task."""
        return TaskReference(dag_id=self.dag_id, task_id=task_id)

    def __repr__(self):
        return f"DAGReference(dag_id='{self.dag_id}')"


# Create the dags module-like object
dags = DAGModule()

