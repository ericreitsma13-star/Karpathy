"""
Skill: Databricks Job Dependency Advisor Skill
Source: https://youtube.com/watch?v=vzu06KGTOrQ
Title: February Databricks Updates: Lakebase Autoscaling, Knowledge Assistant AI, and Sharing to Iceberg
Added: 2026-03-21
"""

class JobDependencyAdvisor:
    """
    A skill to provide advice on managing job dependencies in Databricks.
    This simulates a script an agent could dynamically load into its context,
    as described for Databricks Assistant skills.
    """

    def get_dependency_advice(self, dependency_type: str = "general") -> str:
        """
        Provides advice based on the type of job dependency, aligning with the
        concept of a skill assisting with development tasks and job restarts.
        
        :param dependency_type: Specific area of advice (e.g., "notebook", "dlt", "external").
        :return: A string containing advice.
        """
        advice_map = {
            "notebook": (
                "For Notebook workflow dependencies in Databricks, consider using Databricks "
                "Workflows (Jobs) to orchestrate tasks. Define dependencies between tasks directly "
                "in the job graph to ensure correct execution order. Use `dbutils.jobs.getOutput()` "
                "to pass parameters or results between dependent notebooks if needed. "
                "Ensure robust error handling for upstream tasks."
            ),
            "dlt": (
                "For Delta Live Tables (DLT) dependencies, the DLT pipeline itself manages "
                "table dependencies automatically through its declarative syntax. "
                "Ensure your DLT pipelines are configured to start only after their upstream "
                "data sources (e.g., external ingests) are ready. Monitor pipeline health "
                "and data quality constraints."
            ),
            "external": (
                "When integrating with external systems, ensure robust data ingestion mechanisms "
                "(e.g., Auto Loader for cloud storage, JDBC connectors for databases). "
                "Use external orchestrators like Apache Airflow or Azure Data Factory "
                "if complex cross-platform dependencies exist. Implement idempotency "
                "and retry mechanisms for external calls."
            ),
            "general": (
                "Always clearly define dependencies between tasks. Use a robust orchestration "
                "tool like Databricks Workflows. Monitor job runs and set up alerts for failures. "
                "Consider idempotency for tasks that might be retried."
            ),
        }
        return advice_map.get(dependency_type.lower(), advice_map["general"])

    def suggest_tool_for_dependencies(self) -> str:
        """
        Suggests key tools for managing dependencies within Databricks.
        :return: A string listing suggested tools.
        """
        return (
            "Key tools for managing dependencies in Databricks include: "
            "Databricks Workflows (Jobs) for in-platform orchestration, "
            "Delta Live Tables (DLT) for declarative pipeline dependencies, "
            "and external orchestrators like Apache Airflow for complex cross-platform needs."
        )
