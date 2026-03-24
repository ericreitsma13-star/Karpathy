import sys
from skills.mcp_system import *

import pytest
from unittest.mock import Mock, patch
import requests
import json

# Assuming the implementation code is available as a module named 'proposed_skill'
import proposed_skill

# Test MCPServer
def test_mcp_server_init():
    server = proposed_skill.MCPServer("test_server")
    assert server.server_id == "test_server"
    assert server._tools == {}

def test_mcp_server_register_tool():
    server = proposed_skill.MCPServer("test_server")
    def dummy_tool(): return "dummy"
    server.register_tool("dummy_tool", dummy_tool, "A dummy tool")
    assert "dummy_tool" in server._tools
    assert server._tools["dummy_tool"][1] == "A dummy tool"

def test_mcp_server_register_duplicate_tool_raises_error():
    server = proposed_skill.MCPServer("test_server")
    def dummy_tool(): return "dummy"
    server.register_tool("dummy_tool", dummy_tool, "A dummy tool")
    with pytest.raises(ValueError, match="Tool 'dummy_tool' already registered."):
        server.register_tool("dummy_tool", dummy_tool, "Another description")

def test_mcp_server_get_available_tools():
    server = proposed_skill.MCPServer("test_server")
    def tool1(): pass
    def tool2(): pass
    server.register_tool("tool1", tool1, "Description of tool1")
    server.register_tool("tool2", tool2, "Description of tool2")
    tools = server.get_available_tools()
    assert len(tools) == 2
    assert {"name": "tool1", "description": "Description of tool1"} in tools
    assert {"name": "tool2", "description": "Description of tool2"} in tools

def test_mcp_server_execute_tool():
    server = proposed_skill.MCPServer("test_server")
    def adder(a, b): return a + b
    server.register_tool("adder", adder, "Adds two numbers")
    result = server.execute_tool("adder", 1, 2)
    assert result == 3
    result = server.execute_tool("adder", a=10, b=5)
    assert result == 15

def test_mcp_server_execute_nonexistent_tool_raises_error():
    server = proposed_skill.MCPServer("test_server")
    with pytest.raises(ValueError, match="Tool 'nonexistent_tool' not found on server 'test_server'."):
        server.execute_tool("nonexistent_tool")

# Test MCPClient
def test_mcp_client_init():
    client = proposed_skill.MCPClient()
    assert client._servers == {}

def test_mcp_client_add_server():
    client = proposed_skill.MCPClient()
    server1 = proposed_skill.MCPServer("s1")
    client.add_server("s1", server1)
    assert "s1" in client._servers
    assert client._servers["s1"] is server1

def test_mcp_client_add_duplicate_server_raises_error():
    client = proposed_skill.MCPClient()
    server1 = proposed_skill.MCPServer("s1")
    client.add_server("s1", server1)
    with pytest.raises(ValueError, match="Server 's1' already registered."):
        client.add_server("s1", proposed_skill.MCPServer("s1_duplicate"))

def test_mcp_client_discover_tools():
    client = proposed_skill.MCPClient()
    server1 = proposed_skill.MCPServer("s1")
    server2 = proposed_skill.MCPServer("s2")

    server1.register_tool("tool_s1_a", lambda: "s1a", "Tool A on S1")
    server1.register_tool("tool_s1_b", lambda: "s1b", "Tool B on S1")
    server2.register_tool("tool_s2_c", lambda: "s2c", "Tool C on S2")

    client.add_server("s1", server1)
    client.add_server("s2", server2)

    all_tools = client.discover_tools()
    expected_tools = [
        {"name": "tool_s1_a", "description": "Tool A on S1", "server_id": "s1"},
        {"name": "tool_s1_b", "description": "Tool B on S1", "server_id": "s1"},
        {"name": "tool_s2_c", "description": "Tool C on S2", "server_id": "s2"},
    ]
    assert len(all_tools) == len(expected_tools)
    for tool in expected_tools:
        assert tool in all_tools

def test_mcp_client_call_tool():
    client = proposed_skill.MCPClient()
    server1 = proposed_skill.MCPServer("s1")
    server1.register_tool("multiplier", lambda x, y: x * y, "Multiplies two numbers")
    client.add_server("s1", server1)

    result = client.call_tool("s1", "multiplier", 3, 4)
    assert result == 12

    result = client.call_tool("s1", "multiplier", x=5, y=2)
    assert result == 10

def test_mcp_client_call_tool_nonexistent_server_raises_error():
    client = proposed_skill.MCPClient()
    with pytest.raises(ValueError, match="Server 'nonexistent_server' not found."):
        client.call_tool("nonexistent_server", "any_tool")

def test_mcp_client_call_tool_nonexistent_tool_raises_error():
    client = proposed_skill.MCPClient()
    server1 = proposed_skill.MCPServer("s1")
    client.add_server("s1", server1)
    with pytest.raises(ValueError, match="Tool 'nonexistent_tool' not found on server 's1'."):
        client.call_tool("s1", "nonexistent_tool")

# Test MCPHost
@patch('proposed_skill.MCPHost._mock_llm_tool_chooser')
@patch('proposed_skill.MCPHost._mock_llm_final_answer')
def test_mcp_host_process_query_weather(mock_final_answer, mock_tool_chooser):
    client = proposed_skill.MCPClient()
    server = proposed_skill.MCPServer("weather_server")
    server.register_tool("get_weather", proposed_skill._get_current_weather, "Gets current weather for a location")
    client.add_server("weather_server", server)
    host = proposed_skill.MCPHost(client)

    mock_tool_chooser.return_value = [{'server_id': 'weather_server', 'name': 'get_weather', 'args': ['London']}]
    mock_final_answer.return_value = "The weather is nice in London."

    query = "What's the weather like in London?"
    result = host.process_query(query)

    mock_tool_chooser.assert_called_once_with(query, client.discover_tools())
    mock_final_answer.assert_called_once()
    assert mock_final_answer.call_args[0][0] == query
    # The second argument to mock_final_answer is the tool_results. 
    # We need to ensure _get_current_weather was called and its result passed.
    tool_results_passed = mock_final_answer.call_args[0][1]
    assert 'get_weather' in tool_results_passed
    assert tool_results_passed['get_weather']['location'] == 'London' # Assuming _get_current_weather works without external API for test

    assert result == "The weather is nice in London."

@patch('proposed_skill.MCPHost._mock_llm_tool_chooser')
@patch('proposed_skill.MCPHost._mock_llm_final_answer')
def test_mcp_host_process_query_customer_count(mock_final_answer, mock_tool_chooser):
    client = proposed_skill.MCPClient()
    server = proposed_skill.MCPServer("db_server")
    server.register_tool("get_customer_count", proposed_skill._get_customer_database_count, "Counts customers")
    client.add_server("db_server", server)
    host = proposed_skill.MCPHost(client)

    mock_tool_chooser.return_value = [{'server_id': 'db_server', 'name': 'get_customer_count', 'args': []}]
    mock_final_answer.return_value = "You have 12345 customers."

    query = "How many customers do I have?"
    result = host.process_query(query)

    mock_tool_chooser.assert_called_once_with(query, client.discover_tools())
    mock_final_answer.assert_called_once()
    assert mock_final_answer.call_args[0][0] == query
    tool_results_passed = mock_final_answer.call_args[0][1]
    assert 'get_customer_count' in tool_results_passed
    assert tool_results_passed['get_customer_count']['count'] == 12345
    assert result == "You have 12345 customers."

@patch('proposed_skill.MCPHost._mock_llm_tool_chooser', return_value=[])
@patch('proposed_skill.MCPHost._mock_llm_final_answer')
def test_mcp_host_process_query_no_tool_chosen(mock_final_answer, mock_tool_chooser):
    client = proposed_skill.MCPClient()
    host = proposed_skill.MCPHost(client)

    mock_final_answer.return_value = "I couldn't find a specific answer."

    query = "Tell me a joke."
    result = host.process_query(query)

    mock_tool_chooser.assert_called_once() 
    mock_final_answer.assert_called_once_with(query, {}) 
    assert result == "I couldn't find a specific answer."

# Test the actual tool functions directly 
@patch('requests.get')
def test_get_current_weather_london_via_api(mock_get):
    # Configure mock response for requests.get
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "current_weather": {
            "temperature": 15.0,
            "weathercode": 3,
            "windspeed": 10.0,
            "winddirection": 270,
            "time": "2023-10-27T10:00"
        }
    }
    mock_get.return_value = mock_response

    weather_data = proposed_skill._get_current_weather("London")
    assert weather_data['location'] == 'London'
    assert weather_data['temperature'] == 15.0
    assert 'description' in weather_data
    mock_get.assert_called_once()
    assert "https://api.open-meteo.com/v1/forecast" in mock_get.call_args[0][0]

@patch('requests.get', side_effect=requests.exceptions.Timeout)
def test_get_current_weather_api_timeout(mock_get):
    weather_data = proposed_skill._get_current_weather("New York")
    assert weather_data['location'] == 'New York'
    assert 'error' in weather_data
    assert "timed out" in weather_data['error']
    mock_get.assert_called_once()

@patch('requests.get', side_effect=requests.exceptions.RequestException("Test network error"))
def test_get_current_weather_api_request_exception(mock_get):
    weather_data = proposed_skill._get_current_weather("Tokyo")
    assert weather_data['location'] == 'Tokyo'
    assert 'error' in weather_data
    assert "API request failed" in weather_data['error']
    mock_get.assert_called_once()

@patch('requests.get')
def test_get_current_weather_api_json_decode_error(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "{}", 0)
    mock_get.return_value = mock_response

    weather_data = proposed_skill._get_current_weather("London")
    assert weather_data['location'] == 'London'
    assert 'error' in weather_data
    assert "JSON decode failed" in weather_data['error']
    mock_get.assert_called_once()

@patch('requests.get')
def test_get_current_weather_api_key_error(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"not_current_weather": {}} # Missing expected key
    mock_get.return_value = mock_response

    weather_data = proposed_skill._get_current_weather("London")
    assert weather_data['location'] == 'London'
    assert 'error' in weather_data
    assert "missing 'current_weather' data" in weather_data['error']
    mock_get.assert_called_once()

def test_get_current_weather_unknown_location_no_api_call():
    with patch('requests.get') as mock_get:
        weather_data = proposed_skill._get_current_weather("Mars")
        assert weather_data['location'] == 'Mars'
        assert 'error' in weather_data
        assert "Could not find coordinates" in weather_data['error']
        mock_get.assert_not_called()

def test_get_customer_database_count():
    count_data = proposed_skill._get_customer_database_count()
    assert count_data['count'] == 12345

def test_greet_user():
    greeting_data = proposed_skill._greet_user()
    assert greeting_data['message'] == 'Hello from the MCP server!'
