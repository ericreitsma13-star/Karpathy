import sys
from skills.airflow_dag_with_dependencies import *

import pytest
from datetime import datetime

# The 'dag', 'task_a', 'task_b', 'task_c', 'task_d' objects are assumed to be
# available in the test scope, as if imported from the 'proposed_skill' module.
# The actual Airflow library imports are not repeated here as per instruction,
# but they are implicitly required for the Airflow objects to exist and have their properties.

def test_dag_creation():
    """Test if the DAG is correctly defined with its basic properties."""
    assert dag.dag_id == "my_first_airflow_dag"
    assert dag.start_date == datetime(2023, 1, 1)
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert len(dag.tasks) == 4 # Ensure all four tasks are part of the DAG

def test_task_ids_exist():
    """Test if all expected task IDs are present in the DAG."""
    task_ids = {t.task_id for t in dag.tasks}
    assert "task_A" in task_ids
    assert "task_B" in task_ids
    assert "task_C" in task_ids
    assert "task_D" in task_ids

def test_task_dependencies():
    """Test if task dependencies are set as expected according to the transcript."""
    # Verify: task_a >> [task_b, task_c]
    assert task_a.downstream_task_ids == {"task_B", "task_C"}
    assert task_b.upstream_task_ids == {"task_A"}
    assert task_c.upstream_task_ids == {"task_A"}

    # Verify: task_c >> task_d
    assert task_c.downstream_task_ids == {"task_D"}
    assert task_d.upstream_task_ids == {"task_C"}

    # Ensure tasks without explicit downstream tasks don't have any unexpected ones
    assert task_b.downstream_task_ids == set()
    assert task_d.downstream_task_ids == set()

    # Ensure tasks without explicit upstream tasks don't have any unexpected ones (task_A is the start)
    assert task_a.upstream_task_ids == set()
