import sys
from proposed_skill import *

import pytest
import collections
from datetime import datetime, timedelta
import functools

# Mock task functions
def mock_task_success():
    """A task that always succeeds."""
    return "success"

def mock_task_fail():
    """A task that always fails."""
    raise ValueError("Task failed intentionally")

def mock_task_with_args(arg1, kwarg1="default"):
    """A task that accepts arguments."""
    return f"arg1={arg1}, kwarg1={kwarg1}"

# Assume SimpleWorkflowManager is available from 'proposed_skill'

def test_workflow_init():
    workflow_id = "test_workflow"
    start_date = datetime(2023, 1, 1)
    schedule = timedelta(hours=1)
    workflow = SimpleWorkflowManager(workflow_id, start_date, schedule)
    assert workflow.workflow_id == workflow_id
    assert workflow.start_date == start_date
    assert workflow.schedule_interval == schedule
    assert workflow.tasks == {}
    assert workflow.dependencies == collections.defaultdict(list)

def test_add_task_success():
    workflow = SimpleWorkflowManager("test_workflow")
    workflow.add_task("task_a", mock_task_success)
    assert "task_a" in workflow.tasks
    assert workflow.tasks["task_a"]['callable'] == mock_task_success

def test_add_task_with_args():
    workflow = SimpleWorkflowManager("test_workflow")
    workflow.add_task("task_b", mock_task_with_args, "value1", kwarg1="value2")
    assert "task_b" in workflow.tasks
    assert workflow.tasks["task_b"]['callable'] == mock_task_with_args
    assert workflow.tasks["task_b"]['args'] == ("value1",)
    assert workflow.tasks["task_b"]['kwargs'] == {"kwarg1": "value2"}

def test_add_task_duplicate_id_fails():
    workflow = SimpleWorkflowManager("test_workflow")
    workflow.add_task("task_a", mock_task_success)
    with pytest.raises(ValueError, match="already exists"):
        workflow.add_task("task_a", mock_task_success)

def test_add_task_non_callable_fails():
    workflow = SimpleWorkflowManager("test_workflow")
    with pytest.raises(ValueError, match="must be a function or callable object"):
        workflow.add_task("task_a", "not_a_callable")

def test_add_dependency_success():
    workflow = SimpleWorkflowManager("test_workflow")
    workflow.add_task("task_a", mock_task_success)
    workflow.add_task("task_b", mock_task_success)
    workflow.add_dependency("task_a", "task_b")
    assert "task_b" in workflow.dependencies["task_a"]
    assert "task_a" in workflow.reverse_dependencies["task_b"]

def test_add_dependency_unknown_task_fails():
    workflow = SimpleWorkflowManager("test_workflow")
    workflow.add_task("task_a", mock_task_success)
    with pytest.raises(ValueError, match="Upstream task 'task_x' not found."):
        workflow.add_dependency("task_x", "task_a")
    with pytest.raises(ValueError, match="Downstream task 'task_y' not found."):
        workflow.add_dependency("task_a", "task_y")

def test_add_dependency_cyclic_fails():
    workflow = SimpleWorkflowManager("test_workflow_cycle_add")
    workflow.add_task("task_a", mock_task_success)
    workflow.add_task("task_b", mock_task_success)
    workflow.add_dependency("task_a", "task_b")
    
    with pytest.raises(ValueError, match="creates a cycle"):
        workflow.add_dependency("task_b", "task_a")

def test_run_workflow_no_dependencies():
    workflow = SimpleWorkflowManager("test_workflow_no_dep")
    task_log = []
    def log_task(task_id):
        task_log.append(task_id)
    workflow.add_task("task_1", functools.partial(log_task, "task_1"))
    workflow.add_task("task_2", functools.partial(log_task, "task_2"))
    
    run_output = workflow.run_workflow()
    
    # Check overall status
    assert run_output['overall_status'] == 'success'
    assert len(run_output['task_runs']) == 2
    
    # Check individual task statuses
    task_statuses = {t['task_id']: t['status'] for t in run_output['task_runs']}
    assert task_statuses.get('task_1') == 'success'
    assert task_statuses.get('task_2') == 'success'

    # The current implementation sorts ready_tasks, so we expect a sorted order.
    executed_task_ids = [t['task_id'] for t in run_output['task_runs']]
    assert executed_task_ids == ['task_1', 'task_2'] # Sorted task_ids

def test_run_workflow_with_dependencies():
    workflow = SimpleWorkflowManager("test_workflow_deps")
    execution_order = []

    def task_a():
        execution_order.append("task_a")

    def task_b():
        execution_order.append("task_b")

    def task_c():
        execution_order.append("task_c")

    workflow.add_task("task_a", task_a)
    workflow.add_task("task_b", task_b)
    workflow.add_task("task_c", task_c)

    # a -> b, a -> c
    workflow.add_dependency("task_a", "task_b")
    workflow.add_dependency("task_a", "task_c")

    run_output = workflow.run_workflow()

    assert run_output['overall_status'] == 'success'
    assert len(run_output['task_runs']) == 3

    # Task 'a' must run first
    assert execution_order[0] == "task_a"
    # 'b' and 'c' can run in any order after 'a' (but sorted for this simple impl)
    assert set(execution_order[1:]) == {"task_b", "task_c"}
    
    task_statuses = {t['task_id']: t['status'] for t in run_output['task_runs']}
    assert task_statuses.get('task_a') == 'success'
    assert task_statuses.get('task_b') == 'success'
    assert task_statuses.get('task_c') == 'success'

def test_run_workflow_complex_dependencies():
    workflow = SimpleWorkflowManager("complex_workflow")
    execution_order = []

    def log_task(task_id):
        execution_order.append(task_id)

    workflow.add_task("extract", functools.partial(log_task, "extract"))
    workflow.add_task("transform_1", functools.partial(log_task, "transform_1"))
    workflow.add_task("transform_2", functools.partial(log_task, "transform_2"))
    workflow.add_task("load_dim", functools.partial(log_task, "load_dim"))
    workflow.add_task("load_fact", functools.partial(log_task, "load_fact"))

    workflow.add_dependency("extract", "transform_1")
    workflow.add_dependency("extract", "transform_2")
    workflow.add_dependency("transform_1", "load_dim")
    workflow.add_dependency("transform_2", "load_fact")
    workflow.add_dependency("load_dim", "load_fact") # load_fact also depends on load_dim

    run_output = workflow.run_workflow()

    assert run_output['overall_status'] == 'success'
    assert len(run_output['task_runs']) == 5

    # 'extract' must be first
    assert execution_order[0] == "extract"
    # 'transform_1' and 'transform_2' must be after 'extract'
    assert "extract" not in execution_order[1:]
    # 'load_dim' must be after 'transform_1'
    assert execution_order.index("transform_1") < execution_order.index("load_dim")
    # 'load_fact' must be after 'transform_2' and 'load_dim'
    assert execution_order.index("transform_2") < execution_order.index("load_fact")
    assert execution_order.index("load_dim") < execution_order.index("load_fact")

    task_statuses = {t['task_id']: t['status'] for t in run_output['task_runs']}
    for task_id in ["extract", "transform_1", "transform_2", "load_dim", "load_fact"]:
        assert task_statuses.get(task_id) == 'success'

def test_run_workflow_task_failure():
    workflow = SimpleWorkflowManager("test_workflow_failure")
    execution_order = []

    def task_a_success():
        execution_order.append("task_a")
        return "success"

    def task_b_fail():
        execution_order.append("task_b")
        raise ValueError("Task B failed!")

    def task_c_dependent():
        execution_order.append("task_c")
        return "success"

    workflow.add_task("task_a", task_a_success)
    workflow.add_task("task_b", task_b_fail)
    workflow.add_task("task_c", task_c_dependent)

    # a -> b, b -> c
    workflow.add_dependency("task_a", "task_b")
    workflow.add_dependency("task_b", "task_c")

    run_output = workflow.run_workflow()

    assert run_output['overall_status'] == 'failed_with_errors' # B failed
    assert len(run_output['task_runs']) == 3
    
    task_statuses = {t['task_id']: t['status'] for t in run_output['task_runs']}
    assert task_statuses.get('task_a') == 'success'
    assert task_statuses.get('task_b') == 'failed'
    assert task_statuses.get('task_c') == 'success' # C runs because B is 'completed' (failed but done)

    # Check order
    assert execution_order == ["task_a", "task_b", "task_c"]

def test_simulate_cron_scheduling():
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    schedule_interval = timedelta(days=1)
    workflow = SimpleWorkflowManager("test_cron_workflow", start_date, schedule_interval)
    
    call_times = []
    def record_call_time(execution_time_str):
        call_times.append(datetime.fromisoformat(execution_time_str))

    # Add a task that logs its execution time, passed from the run_workflow log
    workflow.add_task("scheduled_task", functools.partial(record_call_time, execution_time_str=None))

    num_runs = 3
    simulated_logs = workflow.simulate_cron_scheduling(num_runs=num_runs)

    assert len(simulated_logs) == num_runs
    
    expected_times = [start_date + i * schedule_interval for i in range(num_runs)]
    for i, log in enumerate(simulated_logs):
        assert datetime.fromisoformat(log['execution_time']) == expected_times[i]
        assert log['overall_status'] == 'success'

def test_simulate_cron_no_schedule_interval_fails():
    workflow = SimpleWorkflowManager("no_schedule_workflow")
    workflow.add_task("task_1", mock_task_success)
    with pytest.raises(ValueError, match="No schedule interval defined"):
        workflow.simulate_cron_scheduling(num_runs=1)
