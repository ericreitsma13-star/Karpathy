import sys
from skills.jira_ticket_style_analyzer_agent_skill import *

import pytest
import inspect

def test_agent_skill_initialization():
    def dummy_func(arg1: str, arg2: int = 0):
        """A dummy function."""
        pass

    skill = AgentSkill(
        name="DummySkill",
        description="A test skill",
        func=dummy_func
    )

    assert skill.name == "DummySkill"
    assert skill.description == "A test skill"
    assert skill._func == dummy_func
    assert "name" in skill.args_spec
    assert skill.args_spec["name"] == "dummy_func"
    assert "parameters" in skill.args_spec
    assert "arg1" in skill.args_spec["parameters"]
    assert skill.args_spec["parameters"]["arg1"]["type"] == "<class 'str'>"
    assert "arg2" in skill.args_spec["parameters"]
    assert skill.args_spec["parameters"]["arg2"]["type"] == "<class 'int'>"
    assert skill.args_spec["parameters"]["arg2"]["default"] == "0"


def test_agent_skill_get_metadata():
    def another_dummy_func():
        """Another dummy."""
        pass

    skill = AgentSkill(
        name="AnotherDummySkill",
        description="Another test skill",
        func=another_dummy_func
    )
    metadata = skill.get_metadata()

    assert metadata["name"] == "AnotherDummySkill"
    assert metadata["description"] == "Another test skill"
    assert "function_signature" in metadata
    assert metadata["function_signature"]["name"] == "another_dummy_func"


def test_agent_skill_execute():
    def adder_func(a, b):
        return a + b

    skill = AgentSkill(
        name="Adder",
        description="Adds two numbers",
        func=adder_func
    )
    result = skill.execute(5, 3)
    assert result == 8

    result_kwargs = skill.execute(a=10, b=2)
    assert result_kwargs == 12


def test_analyze_ticket_articulation_minimalist():
    ticket_description = "This button is broke. Please fix. 🤷"
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 0
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["concise", "emoji-heavy", "minimalist-urgent"]
    assert result["raw_description_length"] == len(ticket_description)


def test_analyze_ticket_articulation_articulate_structured_with_emoji():
    ticket_description = "Problem Statement: Users are unable to log in due to an authentication timeout issue. Root Cause: The session token expiration is set to 5 minutes, which is too short for typical user workflows. Solution Proposal: Increase session token expiration to 30 minutes and monitor impact. 👍"
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 5  # 3 (detailed) + 4 (structured) - 2 (emoji-heavy) = 5
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["detailed", "emoji-heavy", "structured"]
    assert result["raw_description_length"] == len(ticket_description)


def test_analyze_ticket_articulation_detailed_no_emojis():
    ticket_description = "Our new feature X has a bug where it occasionally crashes on Safari. This seems to happen when the user tries to upload a file larger than 10MB. We need to investigate the file upload handler for Safari specifically. Current assumption is a memory leak or a race condition. "
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 3  # 3 (detailed)
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["detailed"]
    assert result["raw_description_length"] == len(ticket_description)


def test_analyze_ticket_articulation_perfect_articulation():
    ticket_description = "Problem Statement: User authentication flow is occasionally failing under high load. Root Cause: Database connection pooling is exhausted, leading to timeouts. Solution: Implement a circuit breaker pattern and increase connection pool size. This will ensure resilience and improve system stability."
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 7  # 3 (detailed) + 4 (structured) = 7
    assert result["is_articulate"]
    assert result["detected_styles"] == ["detailed", "structured"]
    assert result["raw_description_length"] == len(ticket_description)


def test_analyze_ticket_articulation_empty_string():
    ticket_description = ""
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 0
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["concise"]
    assert result["raw_description_length"] == 0

def test_analyze_ticket_articulation_only_emojis():
    ticket_description = "✨🔥🚀"
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 0  # 0 (concise) - 2 (emoji-heavy) = -2, capped at 0
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["concise", "emoji-heavy"]
    assert result["raw_description_length"] == len(ticket_description)

def test_analyze_ticket_articulation_short_structured():
    ticket_description = "Problem Statement: Button broke. Solution: Fix it."
    result = analyze_ticket_articulation(ticket_description)
    assert result["articulation_score"] == 4 # 0 (concise) + 4 (structured) = 4
    assert not result["is_articulate"]
    assert result["detected_styles"] == ["concise", "structured"]
    assert result["raw_description_length"] == len(ticket_description)
