import collections
from datetime import datetime, timedelta
import functools

class SimpleWorkflowManager:
    """
    A simplified workflow manager demonstrating DAG-like task execution with dependencies,
    mimicking core Airflow concepts (DAG, Tasks, Dependencies, Scheduling) using only Python stdlib.
    Includes basic cycle detection when adding dependencies.
    """
    def __init__(self, workflow_id, start_date=None, schedule_interval=None):
        self.workflow_id = workflow_id
        self.start_date = start_date if start_date else datetime.now()
        self.schedule_interval = schedule_interval # e.g., timedelta(days=1)
        self.tasks = {}  # task_id -> {'callable': callable, 'args': (), 'kwargs': {}}
        self.dependencies = collections.defaultdict(list)  # task_id -> list of downstream_task_ids
        self.reverse_dependencies = collections.defaultdict(list)  # task_id -> list of upstream_task_ids
        self.execution_logs = [] # Stores logs for each run

    def add_task(self, task_id, task_callable, *args, **kwargs):
        """
        Adds a task to the workflow.
        :param task_id: Unique identifier for the task.
        :param task_callable: The Python callable (function) to execute.
        :param args: Positional arguments for the task_callable.
        :param kwargs: Keyword arguments for the task_callable.
        """
        if not callable(task_callable):
            raise ValueError("Task callable must be a function or callable object.")
        if task_id in self.tasks:
            raise ValueError(f"Task with ID '{task_id}' already exists.")
        self.tasks[task_id] = {'callable': task_callable, 'args': args, 'kwargs': kwargs}

    def _has_cycle(self, start_node, target_node):
        """
        Performs a DFS to check if target_node is reachable from start_node in the current graph state.
        This is used to detect cycles when adding a new dependency.
        """
        visited = set()
        stack = [start_node]

        while stack:
            current = stack.pop()
            if current == target_node:
                return True
            if current not in visited:
                visited.add(current)
                # Add neighbors that are not yet visited to the stack
                for neighbor in self.dependencies[current]:
                    if neighbor not in visited: # Ensure we don't revisit in this path
                        stack.append(neighbor)
        return False

    def add_dependency(self, upstream_task_id, downstream_task_id):
        """
        Defines a dependency: downstream_task_id will only run after upstream_task_id completes.
        Includes cycle detection.
        :param upstream_task_id: The task that must complete first.
        :param downstream_task_id: The task that depends on the upstream task.
        """
        if upstream_task_id not in self.tasks:
            raise ValueError(f"Upstream task '{upstream_task_id}' not found.")
        if downstream_task_id not in self.tasks:
            raise ValueError(f"Downstream task '{downstream_task_id}' not found.")
        if downstream_task_id in self.dependencies[upstream_task_id]:
            # Avoid adding duplicate dependencies
            return

        # Temporarily add the dependency to check for cycles
        self.dependencies[upstream_task_id].append(downstream_task_id)
        
        # Check for a cycle: if upstream_task_id is reachable from downstream_task_id
        if self._has_cycle(downstream_task_id, upstream_task_id):
            # If a cycle is detected, revert the dependency addition and raise an error
            self.dependencies[upstream_task_id].pop() # Remove the temporarily added edge
            raise ValueError(f"Adding dependency '{upstream_task_id} -> {downstream_task_id}' creates a cycle.")
        
        # If no cycle, then formally add the reverse dependency
        self.reverse_dependencies[downstream_task_id].append(upstream_task_id)

    def _get_ready_tasks(self, completed_tasks, in_progress_tasks):
        """Identifies tasks that are ready to run."""
        ready_tasks = []
        for task_id in self.tasks:
            if task_id in completed_tasks or task_id in in_progress_tasks:
                continue

            # A task is ready if all its upstream dependencies are completed
            all_dependencies_met = True
            for upstream_task_id in self.reverse_dependencies[task_id]:
                if upstream_task_id not in completed_tasks:
                    all_dependencies_met = False
                    break
            if all_dependencies_met:
                ready_tasks.append(task_id)
        return ready_tasks

    def run_workflow(self, execution_time=None):
        """
        Executes the workflow, respecting task dependencies.
        :param execution_time: The simulated execution time for this run. Defaults to now.
        :return: A dictionary representing the log of the latest run.
        """
        if execution_time is None:
            execution_time = datetime.now()

        current_run_log = []
        completed_tasks = set()
        in_progress_tasks = set() 
        
        while len(completed_tasks) < len(self.tasks):
            ready_tasks = self._get_ready_tasks(completed_tasks, in_progress_tasks)
            
            if not ready_tasks:
                if len(completed_tasks) < len(self.tasks):
                    # This indicates a deadlock (tasks remaining but none can run),
                    # likely due to an unreachable task or logic error (cycles are caught earlier).
                    remaining_tasks = set(self.tasks.keys()) - completed_tasks
                    current_run_log.append({
                        "workflow_id": self.workflow_id,
                        "execution_time": execution_time.isoformat(),
                        "status": "failed_deadlock_or_unreachable",
                        "remaining_tasks": list(remaining_tasks)
                    })
                break # No ready tasks to run, exit loop

            for task_id in sorted(ready_tasks): # Sort for deterministic execution in tests
                in_progress_tasks.add(task_id)
                task_info = self.tasks[task_id]
                task_callable = task_info['callable']
                task_args = task_info['args']
                task_kwargs = task_info['kwargs']
                
                status = "failed"
                error = None
                try:
                    task_callable(*task_args, **task_kwargs)
                    status = "success"
                except Exception as e:
                    error = str(e)
                    # For this simple model, we mark failed tasks as 'completed' 
                    # to allow the workflow to continue attempting other tasks.
                    # A real system would have more nuanced failure handling (retry, skip downstream, etc.).
                finally:
                    in_progress_tasks.remove(task_id)
                    completed_tasks.add(task_id) # Mark as completed whether success or failure
                    current_run_log.append({
                        "task_id": task_id,
                        "status": status,
                        "error": error,
                        "execution_time": execution_time.isoformat()
                    })
        
        final_overall_status = "success"
        if len(completed_tasks) != len(self.tasks): # Not all tasks ran
            final_overall_status = "failed_incomplete"
        elif any(t['status'] == 'failed' for t in current_run_log): # All tasks ran, but some failed
            final_overall_status = "failed_with_errors"

        self.execution_logs.append({
            "workflow_id": self.workflow_id,
            "overall_status": final_overall_status,
            "execution_time": execution_time.isoformat(),
            "task_runs": current_run_log
        })
        return self.execution_logs[-1] # Return the log of the latest run

    def simulate_cron_scheduling(self, num_runs=1):
        """
        Simulates running the workflow multiple times based on its schedule_interval.
        :param num_runs: Number of times to simulate the scheduled run.
        :return: A list of dictionaries, each representing the log of a simulated run.
        """
        if not self.schedule_interval:
            raise ValueError("No schedule interval defined for this workflow. Cannot simulate cron.")

        simulated_runs_logs = []
        current_run_time = self.start_date
        for _ in range(num_runs):
            run_log = self.run_workflow(execution_time=current_run_time)
            simulated_runs_logs.append(run_log)
            current_run_time += self.schedule_interval
        return simulated_runs_logs
