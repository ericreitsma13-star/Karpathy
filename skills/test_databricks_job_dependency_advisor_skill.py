import sys
from skills.databricks_job_dependency_advisor_skill import *

import pytest

def test_get_dependency_advice_general():
    advisor = JobDependencyAdvisor()
    advice = advisor.get_dependency_advice("general")
    assert "orchestration" in advice
    assert "idempotency" in advice

def test_get_dependency_advice_notebook():
    advisor = JobDependencyAdvisor()
    advice = advisor.get_dependency_advice("notebook")
    assert "Workflows" in advice
    assert "dbutils.jobs.getOutput()" in advice

def test_get_dependency_advice_dlt():
    advisor = JobDependencyAdvisor()
    advice = advisor.get_dependency_advice("dlt")
    assert "Delta Live Tables" in advice
    assert "declarative syntax" in advice

def test_get_dependency_advice_external():
    advisor = JobDependencyAdvisor()
    advice = advisor.get_dependency_advice("external")
    assert "Apache Airflow" in advice
    assert "idempotency" in advice

def test_get_dependency_advice_unknown_type_defaults_to_general():
    advisor = JobDependencyAdvisor()
    general_advice = advisor.get_dependency_advice("general")
    unknown_advice = advisor.get_dependency_advice("nonexistent_type")
    assert unknown_advice == general_advice

def test_suggest_tool_for_dependencies():
    advisor = JobDependencyAdvisor()
    tools = advisor.suggest_tool_for_dependencies()
    assert "Databricks Workflows" in tools
    assert "Delta Live Tables" in tools
    assert "Apache Airflow" in tools
