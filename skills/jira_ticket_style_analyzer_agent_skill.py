"""
Skill: Jira Ticket Style Analyzer Agent Skill
Source: https://youtube.com/watch?v=vzu06KGTOrQ
Title: February Databricks Updates: Lakebase Autoscaling, Knowledge Assistant AI, and Sharing to Iceberg
Added: 2026-03-19
"""

import inspect
import json

class AgentSkill:
    """
    Base class for an agent skill, reflecting the concept of
    "instructions, scripts, and assets" bundled together, as described
    for Databricks Assistant skills.

    Skills can be dynamically loaded and provide callable actions
    with associated documentation/instructions.
    """
    def __init__(self, name: str, description: str, func: callable):
        self.name = name
        self.description = description
        self._func = func
        self.args_spec = self._get_function_signature(func)

    def _get_function_signature(self, func: callable) -> dict:
        """Extracts the function signature for LLM consumption, similar to tool definitions."""
        signature = inspect.signature(func)
        params = {}
        for name, param in signature.parameters.items():
            param_info = {"type": str(param.annotation) if param.annotation != inspect.Parameter.empty else "Any"}
            if param.default != inspect.Parameter.empty:
                param_info["default"] = str(param.default)
            params[name] = param_info
        return {
            "name": func.__name__,
            "doc": func.__doc__,
            "parameters": params
        }

    def execute(self, *args, **kwargs):
        """Executes the skill's underlying function."""
        return self._func(*args, **kwargs)

    def get_metadata(self) -> dict:
        """Returns metadata about the skill for agent consumption, including its function signature."""
        return {
            "name": self.name,
            "description": self.description,
            "function_signature": self.args_spec
        }

# --- Concrete Skill Implementation: Jira Ticket Style Analyzer ---

def analyze_ticket_articulation(ticket_description: str) -> dict:
    """
    Analyzes the articulation style of a Jira ticket description.
    Rates the ticket's articulation based on length, complexity, and
    presence of certain keywords or emojis (simulated).

    This skill is designed to help an agent understand communication patterns
    in Jira tickets, potentially for use cases like matchmaking or identifying
    well-articulated problems based on the transcript's discussion.

    Args:
        ticket_description (str): The text content of the Jira ticket.

    Returns:
        dict: A dictionary containing 'articulation_score' (0-10),
              'is_articulate' (bool), and 'detected_styles' (list of str).
    """
    score = 0
    detected_styles = []

    description_lower = ticket_description.lower()

    # Style: Detailed vs. Concise
    if len(ticket_description) > 100:
        score += 3
        detected_styles.append("detailed")
    else:
        detected_styles.append("concise")

    # Style: Structured - based on keywords like 'Problem Statement'
    structured_keywords = ["problem statement", "root cause", "solution proposal", "impact analysis"]
    if any(word in description_lower for word in structured_keywords):
        score += 4
        detected_styles.append("structured")

    # Style: Urgent/Minimalist - based on phrases like 'please fix'
    if "please fix" in description_lower:
        score -= 3
        detected_styles.append("minimalist-urgent")

    # Style: Emoji-heavy - as noted for less articulate descriptions
    emojis = ["😊", "👍", "🤷", "✨", "🔥", "🚀", "💡", "✅", "❌", "🤔", "🤯", "🥳", "🤩"]
    if any(emoji in ticket_description for emoji in emojis):
        score -= 2
        detected_styles.append("emoji-heavy")

    score = max(0, min(10, score)) # Cap score between 0 and 10
    is_articulate = score >= 7

    return {
        "articulation_score": score,
        "is_articulate": is_articulate,
        "detected_styles": sorted(list(set(detected_styles))), # Sort for consistent order
        "raw_description_length": len(ticket_description)
    }

# Instantiate the skill object for an agent to discover and use.
# This specific instance represents the `JiraTicketStyleAnalyzer` skill.
jira_style_analyzer_skill = AgentSkill(
    name="JiraTicketStyleAnalyzer",
    description="Analyzes the articulation and style of a given Jira ticket description "
                "to infer communication patterns (e.g., structured, emoji-heavy, minimalist) "
                "for potential matchmaking or quality assessment.",
    func=analyze_ticket_articulation
)

if __name__ == "__main__":
    print("--- Skill Metadata ---")
    print(json.dumps(jira_style_analyzer_skill.get_metadata(), indent=2))

    print("\n--- Skill Execution Examples ---")
    ticket1 = "This button is broke. Please fix. 🤷"
    print(f"\nAnalyzing ticket 1: '{ticket1}'")
    result1 = jira_style_analyzer_skill.execute(ticket1)
    print(json.dumps(result1, indent=2))

    ticket2 = "Problem Statement: Users are unable to log in due to an authentication timeout issue. Root Cause: The session token expiration is set to 5 minutes, which is too short for typical user workflows. Solution Proposal: Increase session token expiration to 30 minutes and monitor impact. 👍"
    print(f"\nAnalyzing ticket 2: '{ticket2}'")
    result2 = jira_style_analyzer_skill.execute(ticket2)
    print(json.dumps(result2, indent=2))

    ticket3 = "Our new feature X has a bug where it occasionally crashes on Safari. This seems to happen when the user tries to upload a file larger than 10MB. We need to investigate the file upload handler for Safari specifically. Current assumption is a memory leak or a race condition. "
    print(f"\nAnalyzing ticket 3: '{ticket3}'")
    result3 = jira_style_analyzer_skill.execute(ticket3)
    print(json.dumps(result3, indent=2))

    ticket4 = "Problem Statement: User authentication flow is occasionally failing under high load. Root Cause: Database connection pooling is exhausted, leading to timeouts. Solution: Implement a circuit breaker pattern and increase connection pool size. This will ensure resilience and improve system stability."
    print(f"\nAnalyzing ticket 4: '{ticket4}'")
    result4 = jira_style_analyzer_skill.execute(ticket4)
    print(json.dumps(result4, indent=2))
