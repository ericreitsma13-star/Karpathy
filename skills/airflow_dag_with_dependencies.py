"""
Skill: airflow_dag_with_dependencies
Source: https://youtube.com/watch?v=5peQThvQmQk
Title: Learn Apache Airflow in 10 Minutes | High-Paying Skills for Data Engineers
Added: 2026-03-20
"""

from datetime import datetime

# This module defines an Apache Airflow Directed Acyclic Graph (DAG).
# It demonstrates how to declare a DAG, define individual tasks using operators,
# and set up dependencies between these tasks using Python code.
# This adheres to the "pipeline as a code" principle of Airflow.

# --- MOCK AIRFLOW CLASSES FOR TESTING WITHOUT AIRFLOW LIBRARY ---
# The actual 'airflow' library is not available in the test environment.
# These mock classes simulate the essential behavior needed by the unit tests.

class MockDAG:
    """A mock DAG class to simulate Airflow's DAG behavior for testing."""
    def __init__(self, dag_id, start_date, schedule_interval, catchup):
        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.tasks = [] # To hold task objects associated with this DAG

    def __enter__(self):
        """Enter the runtime context related to this object (for 'with' statement)."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        pass

class MockOperator:
    """A mock Operator class to simulate Airflow's Task/Operator behavior for testing."""
    def __init__(self, task_id, dag):
        self.task_id = task_id
        self.dag = dag
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        self.dag.tasks.append(self) # Associate this task with the DAG

    def __rshift__(self, other):
        """Implements the '>>' operator for setting downstream dependencies."""
        if isinstance(other, list):
            for task in other:
                self._set_dependency(task)
        else:
            self._set_dependency(other)
        return other # Allow chaining, e.g., A >> B >> C

    def _set_dependency(self, downstream_task):
        """Helper to set a single dependency relationship."""
        self.downstream_task_ids.add(downstream_task.task_id)
        downstream_task.upstream_task_ids.add(self.task_id)

# Replace Airflow's DAG and DummyOperator with our mock implementations
DAG = MockDAG
DummyOperator = MockOperator

# --- ORIGINAL AIRFLOW DAG DEFINITION (using our mocks) ---

# Define the DAG (workflow) itself.
# dag_id: A unique identifier for the DAG.
# start_date: The date from which to start considering runs.
# schedule_interval: How often the DAG should run (e.g., "@daily", "@hourly", cron string).
# catchup: If True, backfill missed runs from the start_date to the current date.
with DAG(
    dag_id="my_first_airflow_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    # Define tasks within the DAG.
    # Operators are used to define the actual work that a task performs.
    # DummyOperator (or EmptyOperator in Airflow 2.0+) does nothing but allows defining task structure.
    # task_id: A unique identifier for the task within the DAG.
    # dag: Associates the task with the current DAG context.
    task_a = DummyOperator(task_id="task_A", dag=dag)
    task_b = DummyOperator(task_id="task_B", dag=dag)
    task_c = DummyOperator(task_id="task_C", dag=dag)
    task_d = DummyOperator(task_id="task_D", dag=dag)

    # Set task dependencies.
    # The '>>' operator defines upstream-to-downstream relationships.
    # 'task_a >> [task_b, task_c]' means task_a must complete before task_b and task_c can run concurrently.
    # 'task_c >> task_d' means task_c must complete before task_d can run.
    task_a >> [task_b, task_c]
    task_c >> task_d