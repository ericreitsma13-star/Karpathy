from datetime import datetime, timedelta
import inspect # Used for internal validation, not a user-facing dependency

class _MockOperator:
    """
    A conceptual representation of an Airflow operator for demonstrating
    DAG and task definition principles without the 'airflow' library dependency.
    """
    def __init__(self, task_id: str, dag: "_MockDAG" = None):
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("task_id must be a non-empty string.")
        self.task_id = task_id
        self.dag = dag
        if dag:
            dag.add_task(self)
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()

    def __rshift__(self, other):
        """
        Simulates the Airflow dependency operator `>>`.
        Connects this task to one or more downstream tasks.
        """
        if isinstance(other, list):
            for o in other:
                self._add_dependency(self, o)
        else:
            self._add_dependency(self, other)
        return other

    def __lshift__(self, other):
        """
        Simulates the Airflow dependency operator `<<`.
        Connects one or more upstream tasks to this task.
        """
        if isinstance(other, list):
            for o in other:
                self._add_dependency(o, self)
        else:
            self._add_dependency(other, self)
        return other

    def _add_dependency(self, upstream: "_MockOperator", downstream: "_MockOperator"):
        """Internal method to register dependencies."""
        if upstream.dag != downstream.dag:
            raise ValueError("Tasks must belong to the same DAG to establish dependencies.")
        upstream.downstream_task_ids.add(downstream.task_id)
        downstream.upstream_task_ids.add(upstream.task_id)

    def __repr__(self):
        return f"MockOperator(task_id='{self.task_id}')"


class MockDummyOperator(_MockOperator):
    """A conceptual DummyOperator that does nothing, for illustrating DAG structure."""
    def __init__(self, task_id: str, dag: "_MockDAG" = None):
        super().__init__(task_id=task_id, dag=dag)

class MockPythonOperator(_MockOperator):
    """A conceptual PythonOperator for illustrating Python function execution within a DAG."""
    def __init__(self, task_id: str, python_callable, dag: "_MockDAG" = None):
        super().__init__(task_id=task_id, dag=dag)
        if not callable(python_callable):
            raise TypeError("python_callable must be a callable function.")
        self.python_callable = python_callable

    def execute(self):
        """Simulates the execution of the Python callable."""
        print(f"Executing Python task: {self.task_id}")
        # In a real Airflow PythonOperator, `op_args` or `op_kwargs` would pass data.
        # This simulation focuses on structure, not data passing between tasks.
        self.python_callable()

class MockBashOperator(_MockOperator):
    """A conceptual BashOperator for illustrating shell command execution within a DAG."""
    def __init__(self, task_id: str, bash_command: str, dag: "_MockDAG" = None):
        super().__init__(task_id=task_id, dag=dag)
        if not isinstance(bash_command, str) or not bash_command:
            raise TypeError("bash_command must be a non-empty string.")
        self.bash_command = bash_command

    def execute(self):
        """Simulates the execution of the Bash command."""
        print(f"Executing Bash task: {self.task_id} with command: '{self.bash_command}'")
        # In a real scenario, this would use subprocess.run, but we avoid non-stdlib for simplicity.


class _MockDAGContext:
    """
    A context manager to simulate `with DAG(...) as dag:` syntax,
    making the current DAG instance available for implicit task association.
    """
    _current_dag = None

    def __init__(self, dag_instance):
        self.dag_instance = dag_instance

    def __enter__(self):
        _MockDAGContext._current_dag = self.dag_instance
        return self.dag_instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        _MockDAGContext._current_dag = None

class _MockDAG:
    """
    A conceptual representation of an Apache Airflow DAG (Directed Acyclic Graph).
    This class demonstrates how a workflow is defined as a collection of tasks
    and their dependencies, without requiring the 'airflow' library.
    """
    def __init__(
        self,
        dag_id: str,
        start_date: datetime,
        schedule_interval: timedelta = None,
        catchup: bool = False,
        tags: list[str] = None
    ):
        if not isinstance(dag_id, str) or not dag_id:
            raise ValueError("dag_id must be a non-empty string.")
        if not isinstance(start_date, datetime):
            raise TypeError("start_date must be a datetime object.")

        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.tags = tags if tags is not None else []
        self.tasks: dict[str, _MockOperator] = {}

    def add_task(self, task: _MockOperator):
        if not isinstance(task, _MockOperator):
            raise TypeError("Only _MockOperator instances can be added to a DAG.")
        if task.task_id in self.tasks:
            raise ValueError(f"Task with ID '{task.task_id}' already exists in DAG '{self.dag_id}'.")
        self.tasks[task.task_id] = task

    def get_task(self, task_id: str) -> _MockOperator:
        return self.tasks.get(task_id)

    @property
    def task_ids(self) -> list[str]:
        return list(self.tasks.keys())

    def __enter__(self):
        return _MockDAGContext(self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        _MockDAGContext(self).__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self):
        return f"MockDAG(dag_id='{self.dag_id}', tasks={len(self.tasks)})"

# Aliases for external use, mimicking Airflow's public API structure
DAG = _MockDAG
DummyOperator = MockDummyOperator
PythonOperator = MockPythonOperator
BashOperator = MockBashOperator

# --- Example Usage / Data Pipeline Definition Functions ---

def extract_data_from_api():
    """Simulates extracting data from an external API."""
    print("STAGE: Extracting data...")
    # In a real scenario, this would use 'requests' or a database client
    return {"data": "some_raw_data"}

def transform_and_clean_data():
    """Simulates transforming and cleaning raw data.
    Simplified to not take arguments for structural demo purposes.
    """
    print(f"STAGE: Transforming and aggregating data...")
    # Apply various transformations
    return {"processed_data": "DUMMY_PROCESSED_DATA"}

def load_data_to_storage():
    """Simulates loading processed data to a target storage."""
    print(f"STAGE: Loading data to target location...")
    # In a real scenario, this would write to Delta Lake, S3, etc.
    return True

def define_etl_workflow_dag() -> DAG:
    """
    Defines a conceptual ETL (Extract, Transform, Load) workflow
    using Airflow-inspired DAG and operator syntax.

    This function demonstrates how to:
    1. Define a DAG with a unique ID, start date, and schedule.
    2. Create different types of tasks (dummy, python, bash).
    3. Establish task dependencies to ensure sequential or parallel execution.

    Note: This implementation uses conceptual classes (DAG, DummyOperator etc.)
    to adhere to the constraint of not using the actual 'airflow' library.
    It serves to illustrate the structure and principles of Airflow DAGs.
    """
    with DAG(
        dag_id="etl_data_processing_workflow_conceptual",
        start_date=datetime(2023, 1, 1, 0, 0, 0),
        schedule_interval=timedelta(days=1),  # Run daily
        catchup=False,  # Do not run for past missed schedules
        tags=["data_engineering", "etl", "conceptual"]
    ) as dag:
        # Task 1: Start the pipeline (Dummy Task)
        start_task = DummyOperator(task_id="start_pipeline", dag=dag)

        # Task 2: Extract data from an API (Python Task)
        extract_task = PythonOperator(
            task_id="extract_from_source_api",
            python_callable=extract_data_from_api,
            dag=dag
        )

        # Task 3: Transform and aggregate data (Python Task)
        transform_task = PythonOperator(
            task_id="transform_and_aggregate_data",
            python_callable=transform_and_clean_data,
            dag=dag
        )

        # Task 4: Load data to target (Python Task)
        load_task = PythonOperator(
            task_id="load_to_delta_lake",
            python_callable=load_data_to_storage,
            dag=dag
        )

        # Task 5: Run a shell command (Bash Task)
        # Example: check if a file exists or run a simple cleanup
        check_disk_space_task = BashOperator(
            task_id="check_disk_space",
            bash_command="echo 'Checking disk space...'", # or 'df -h'
            dag=dag
        )

        # Task 6: End the pipeline (Dummy Task)
        end_task = DummyOperator(task_id="end_pipeline", dag=dag)

        # Define the task dependencies as described in Airflow
        # Example from transcript: "task1 >> [task2, task3] >> task4"
        start_task >> extract_task
        extract_task >> transform_task
        transform_task >> [load_task, check_disk_space_task] # Parallel execution
        [load_task, check_disk_space_task] >> end_task # Both must complete before end_task

    return dag

# If run as a script, demonstrate DAG creation and conceptual execution
if __name__ == "__main__":
    print("Defining a conceptual Airflow DAG...")
    my_dag = define_etl_workflow_dag()
    print(f"\nConceptual DAG '{my_dag.dag_id}' created with {len(my_dag.tasks)} tasks.")
    print("Tasks in DAG:", my_dag.task_ids)

    print("\nTask Dependencies (Upstream -> Downstream):")
    for task_id, task in my_dag.tasks.items():
        print(f"  {task_id}:")
        print(f"    Upstream: {list(task.upstream_task_ids)}")
        print(f"    Downstream: {list(task.downstream_task_ids)}")

    print("\nConceptual execution simulation:")
    extract_data_from_api()
    transform_and_clean_data()
    load_data_to_storage()
    # For BashOperator, actual execution would be via subprocess, here just print
    # For actual Airflow, tasks would run in order determined by scheduler.
    print("STAGE: Pipeline completed successfully (conceptually)!")
