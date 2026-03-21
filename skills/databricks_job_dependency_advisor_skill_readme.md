# Databricks Job Dependency Advisor Skill

**Source:** [February Databricks Updates: Lakebase Autoscaling, Knowledge Assistant AI, and Sharing to Iceberg](https://youtube.com/watch?v=vzu06KGTOrQ)
**Added:** 2026-03-21

This `README.md` provides documentation for the `JobDependencyAdvisor` skill.

---

# Databricks Job Dependency Advisor Skill

This repository contains a Python class simulating the "Databricks Job Dependency Advisor" skill, a conceptual feature for the Databricks Assistant.

## What this is
This is a simulated AI skill designed to provide data engineers with practical advice and best practices for managing dependencies between various jobs and tasks within Databricks. It aligns with the Databricks Assistant's goal of enhancing developer productivity and operational efficiency.

## The problem it solves
Data engineers often face complexity in orchestrating interdependent data pipelines. This skill aims to streamline development and troubleshooting by offering:
*   **Contextual guidance** on defining and managing dependencies for different scenarios (e.g., Notebook-based workflows, Delta Live Tables, external system integrations).
*   **Recommendations on suitable tools** for dependency management within and outside Databricks.
*   **Best practices** to build robust, fault-tolerant pipelines, aiding in tasks like job restarts and understanding failure impacts.

## How to use it (with a short code example)
This class provides a local simulation of how a Databricks Assistant skill might function. In a real Databricks environment, the Assistant would directly invoke such a skill based on user prompts.

```python
# Assuming JobDependencyAdvisor class is available in your current context or imported
# from your_module import JobDependencyAdvisor

advisor = JobDependencyAdvisor()

# Get general advice on dependencies
print("--- General Dependency Advice ---")
print(advisor.get_dependency_advice())
print("\n")

# Get specific advice for Notebook-based dependencies
print("--- Notebook Dependency Advice ---")
print(advisor.get_dependency_advice("notebook"))
print("\n")

# Get specific advice for Delta Live Tables (DLT) dependencies
print("--- DLT Dependency Advice ---")
print(advisor.get_dependency_advice("dlt"))
print("\n")

# Get suggested tools for managing dependencies
print("--- Suggested Tools for Dependencies ---")
print(advisor.suggest_tool_for_dependencies())
```

## What real-world tool this relates to
This skill directly relates to the **Databricks Job Dependency Advisor**, an AI-powered feature for the Databricks Assistant announced to help users manage job dependencies. It's envisioned as an intelligent agent within the Databricks platform that provides context-aware guidance for pipeline development, debugging, and operational tasks.

## Limitations
*   **Conceptual Simulation:** This Python class is a static representation, simulating the *concept* of an AI skill, not the actual dynamic feature within the Databricks Assistant.
*   **Static Advice:** The advice provided is pre-programmed within the class. The actual Databricks Assistant feature would likely use large language models (LLMs) to generate more dynamic, context-specific, and adaptive advice.
*   **No Active Management:** This skill *advises* on dependencies; it does not actively manage, configure, or interact with Databricks jobs or their underlying dependencies.
*   **Limited Scope:** The scope of advice is restricted to the predefined `dependency_type` categories and hardcoded suggestions.