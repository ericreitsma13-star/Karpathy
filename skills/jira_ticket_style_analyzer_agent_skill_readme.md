# Jira Ticket Style Analyzer Agent Skill

**Source:** [February Databricks Updates: Lakebase Autoscaling, Knowledge Assistant AI, and Sharing to Iceberg](https://youtube.com/watch?v=vzu06KGTOrQ)
**Added:** 2026-03-19

This README provides documentation for the `Jira Ticket Style Analyzer Agent Skill`, extracted from the February Databricks Updates video.

---

# Jira Ticket Style Analyzer Agent Skill

## What this is
This is an `AgentSkill` designed to analyze the articulation style and content patterns within a Jira ticket description. It quantifies aspects like detail, structure, urgency indicators, and the presence of emojis to provide an "articulation score" and identify specific communication styles.

It serves as a modular component for AI agents (like Databricks Assistant) to programmatically understand the nature and quality of human communication captured in text, helping them make more informed decisions or take appropriate actions.

## The problem it solves
For data engineers and AI agents working with operational data (like Jira tickets), this skill addresses several challenges:

1.  **AI Agent Understanding:** Helps an AI assistant (e.g., a Databricks Knowledge Assistant) to quickly grasp the *tone* and *quality* of a problem description. For instance, an agent might prioritize "minimalist-urgent" tickets or route "detailed-structured" ones to a specific expert.
2.  **Automated Triage & Routing (Conceptual):** While currently a scoring mechanism, the insights derived (e.g., "is_articulate", "detected_styles") could conceptually be used in automated systems to:
    *   Prioritize highly articulate problem statements.
    *   Flag overly concise or emoji-heavy descriptions for human review or clarification requests.
    *   Route tickets based on identified patterns (e.g., a "structured" ticket might go to a technical lead).
3.  **Communication Pattern Analysis:** Offers a simple way to audit and understand common communication patterns within an organization's Jira tickets, potentially informing training or documentation standards.

## How to use it
The skill is instantiated as an `AgentSkill` object, making it discoverable and executable by an AI agent. You provide a Jira ticket description, and it returns a structured analysis.

```python
import inspect
import json

class AgentSkill:
    def __init__(self, name: str, description: str, func: callable):
        self.name = name
        self.description = description
        self._func = func
        self.args_spec = self._get_function_signature(func)

    def _get_function_signature(self, func: callable) -> dict:
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
        return self._func(*args, **kwargs)

    def get_metadata(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "function_signature": self.args_spec
        }

def analyze_ticket_articulation(ticket_description: str) -> dict:
    """
    Analyzes the articulation style of a Jira ticket description.
    Rates the ticket's articulation based on length, complexity, and
    presence of certain keywords or emojis (simulated).

    Args:
        ticket_description (str): The text content of the Jira ticket.

    Returns:
        dict: A dictionary containing 'articulation_score' (0-10),
              'is_articulate' (bool), and 'detected_styles' (list of str).
    """
    score = 0
    detected_styles = []

    description_lower = ticket_description.lower()

    if len(ticket_description) > 100:
        score += 3
        detected_styles.append("detailed")
    else:
        detected_styles.append("concise")

    structured_keywords = ["problem statement", "root cause", "solution proposal", "impact analysis"]
    if any(word in description_lower for word in structured_keywords):
        score += 4
        detected_styles.append("structured")

    if "please fix" in description_lower:
        score -= 3
        detected_styles.append("minimalist-urgent")

    emojis = ["😊", "👍", "🤷", "✨", "🔥", "🚀", "💡", "✅", "❌", "🤔", "🤯", "🥳", "🤩"]
    if any(emoji in ticket_description for emoji in emojis):
        score -= 2
        detected_styles.append("emoji-heavy")

    score = max(0, min(10, score))
    is_articulate = score >= 7

    return {
        "articulation_score": score,
        "is_articulate": is_articulate,
        "detected_styles": sorted(list(set(detected_styles))),
        "raw_description_length": len(ticket_description)
    }

# Instantiate the skill object
jira_style_analyzer_skill = AgentSkill(
    name="JiraTicketStyleAnalyzer",
    description="Analyzes the articulation and style of a given Jira ticket description "
                "to infer communication patterns (e.g., structured, emoji-heavy, minimalist) "
                "for potential matchmaking or quality assessment.",
    func=analyze_ticket_articulation
)

# Example Usage:
ticket_example_1 = "This button is broke. Please fix. 🤷"
print(f"Analyzing ticket: '{ticket_example_1}'")
result_1 = jira_style_analyzer_skill.execute(ticket_example_1)
print(json.dumps(result_1, indent=2))

ticket_example_2 = "Problem Statement: Users are unable to log in due to an authentication timeout. Root Cause: Session token expiration is too short. Solution Proposal: Increase session token expiration to 30 minutes and monitor impact."
print(f"\nAnalyzing ticket: '{ticket_example_2}'")
result_2 = jira_style_analyzer_skill.execute(ticket_example_2)
print(json.dumps(result_2, indent=2))
```

## What real-world tool this relates to
This skill conceptually relates to:

*   **LLM Agents / AI Assistants:** Directly demonstrates how AI assistants (like Databricks Assistant, OpenAI Function Calling, LangChain agents) can integrate custom "tools" or "skills" to perform specific, predefined tasks.
*   **Text Analytics / Natural Language Processing (NLP):** The underlying function performs a basic form of text analysis, similar to what more sophisticated NLP models or sentiment analysis tools do, but with a focus on specific communication styles.
*   **Jira Automation / Service Desk Automation:** Features in platforms like Jira Service Management that might trigger workflows based on keyword detection or the structure of incoming requests.
*   **Communication Style Guides / Linter Tools:** Similar to how code linters ensure code quality, this could be extended to "lint" communication for adherence to organizational standards.

## Limitations
*   **Simulated Logic:** The current analysis is based on simple keyword matching, length checks, and emoji detection. It is a *simulated* version of style analysis and does not use advanced Natural Language Understanding (NLU) or Machine Learning models.
*   **Limited Style Detection:** Only detects a very narrow set of predefined styles (detailed, concise, structured, minimalist-urgent, emoji-heavy). It would struggle with nuanced language or complex communication patterns.
*   **Arbitrary Scoring:** The `articulation_score` is based on arbitrarily assigned points for different characteristics and may not align with subjective human judgment of "articulate."
*   **No Contextual Understanding:** It treats each ticket description in isolation and doesn't understand the broader context of the project, user, or urgency outside of explicit keywords.
*   **Lack of Sophistication:** It's not a general-purpose text classification tool; it's tailored to a very specific (and simplified) interpretation of Jira ticket styles.