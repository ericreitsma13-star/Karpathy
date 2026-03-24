# mcp_system

**Source:** [What is MCP? Integrate AI Agents with Databases & APIs](https://youtube.com/watch?v=eur8dUO9mvE)
**Added:** 2026-03-24

```markdown
# mcp_system: Multi-Component Protocol for AI Agent Integration

## What this is

This project implements a conceptual "Multi-Component Protocol" (MCP) system designed to bridge Large Language Models (LLMs) with diverse data sources and APIs. It's a foundational framework for allowing AI agents to intelligently interact with your organization's operational data and services.

The system comprises three core components:

*   **`MCPServer`**: A component that registers and hosts "tools." Each tool is a specific function that can interact with an external data source (e.g., a database, data lake, sensor stream) or an API (e.g., a weather service, CRM, internal microservice). Servers can be logically grouped, for example, a "CustomerDataServer" or "ExternalAPIServer."
*   **`MCPClient`**: Connects to multiple `MCPServer` instances. Its role is to discover all available tools across these servers and provide a unified interface for executing them.
*   **`MCPHost`**: The orchestration layer. It takes a user's natural language query, interacts with a (mocked) LLM to decide which tools to use, calls the necessary tools via the `MCPClient`, collects the results, and then feeds these results back to the LLM to synthesize a final, human-readable answer.

## The problem it solves

Data engineers often work in complex environments where valuable data and functionalities are scattered across various systems: SQL databases, NoSQL stores, data lakes, internal APIs, and third-party SaaS applications. When integrating AI agents, particularly LLMs, these challenges amplify:

1.  **LLMs are disconnected from proprietary data**: LLMs inherently lack direct access to an organization's internal, real-time data or specialized APIs.
2.  **Bridging the gap for AI**: There's a need for a standardized way for LLMs to "use" external capabilities to answer questions, automate tasks, or retrieve information beyond their training data.
3.  **Tool discovery and management**: As the number of data sources and APIs grows, so does the complexity of finding, understanding, and correctly invoking the right tool for a given query.
4.  **Orchestration complexity**: Managing the flow from a user query to tool selection, execution, and final answer synthesis requires a robust framework.

The `mcp_system` provides a structured approach to encapsulate data access and API calls into discrete "tools," making them discoverable and executable by an intelligent orchestrator (like an LLM agent).

## How to use it

To demonstrate the `mcp_system`, we'll set up a few servers with dummy tools and process a user query.

```python
import requests
import json
import time
from typing import Dict, Any, List, Callable, Tuple

# Assume the MCPServer, MCPClient, MCPHost, and dummy tool functions
# (_get_current_weather, _get_customer_database_count, _greet_user)
# are defined as in the provided code snippet.

# --- Dummy tool implementations (as provided in the problem description) ---
def _get_current_weather(location: str) -> Dict[str, Any]:
    """Simulates fetching weather data from an external API using requests."""
    lat_lon_map = {
        "london": (51.5074, 0.1278),
        "new york": (40.7128, -74.0060),
        "tokyo": (35.6895, 139.6917)
    }
    location_lower = location.lower()
    if location_lower not in lat_lon_map:
        return {"location": location, "error": "Could not find coordinates for this location."}
    lat, lon = lat_lon_map[location_lower]
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true&timezone=Europe%2FBerlin"
    try:
        response = requests.get(api_url, timeout=5)
        response.raise_for_status()
        data = response.json()['current_weather']
        return {
            "location": location,
            "temperature": data['temperature'],
            "description": f"Weather code {data['weathercode']}",
            "windspeed": data['windspeed']
        }
    except Exception as e:
        return {"location": location, "error": f"API request failed: {e}"}

def _get_customer_database_count() -> Dict[str, int]:
    """Simulates querying a database for customer count."""
    time.sleep(0.1)
    return {"count": 12345}

def _greet_user() -> Dict[str, str]:
    """A simple tool to greet the user."""
    return {"message": "Hello from the MCP server!"}

# --- MCP Component Classes (as provided in the problem description) ---
# Paste the full class definitions for MCPServer, MCPClient, MCPHost here
# if running this as a standalone snippet. For a README, assume they exist.

class MCPServer:
    """Simulates an MCP Server component."""
    def __init__(self, server_id: str):
        self.server_id = server_id
        self._tools: Dict[str, Tuple[Callable, str]] = {}
    def register_tool(self, name: str, func: Callable, description: str):
        if name in self._tools: raise ValueError(f"Tool '{name}' already registered.")
        self._tools[name] = (func, description)
    def get_available_tools(self) -> List[Dict[str, str]]:
        return [{"name": name, "description": desc} for name, (_, desc) in self._tools.items()]
    def execute_tool(self, name: str, *args, **kwargs) -> Any:
        if name not in self._tools: raise ValueError(f"Tool '{name}' not found on server '{self.server_id}'.")
        func, _ = self._tools[name]
        return func(*args, **kwargs)

class MCPClient:
    """Simulates an MCP Client component."""
    def __init__(self):
        self._servers: Dict[str, MCPServer] = {}
    def add_server(self, server_id: str, server_instance: MCPServer):
        if server_id in self._servers: raise ValueError(f"Server '{server_id}' already registered.")
        self._servers[server_id] = server_instance
    def discover_tools(self) -> List[Dict[str, Any]]:
        all_tools = []
        for server_id, server in self._servers.items():
            for tool_info in server.get_available_tools():
                tool_info['server_id'] = server_id
                all_tools.append(tool_info)
        return all_tools
    def call_tool(self, server_id: str, tool_name: str, *args, **kwargs) -> Any:
        server = self._servers.get(server_id)
        if not server: raise ValueError(f"Server '{server_id}' not found.")
        return server.execute_tool(tool_name, *args, **kwargs)

class MCPHost:
    """Simulates an MCP Host application (e.g., chat app, IDE assistant)."""
    def __init__(self, client: MCPClient):
        self._client = client
    def _mock_llm_tool_chooser(self, query: str, available_tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chosen_tools = []
        query_lower = query.lower()
        for tool in available_tools:
            if "weather" in query_lower and tool['name'] == 'get_weather':
                location = query_lower.split("weather in ")[-1].split("?")[0].strip()
                chosen_tools.append({'server_id': tool['server_id'], 'name': tool['name'], 'args': [location]})
                break
            elif "customers" in query_lower and tool['name'] == 'get_customer_count':
                chosen_tools.append({'server_id': tool['server_id'], 'name': tool['name'], 'args': []})
                break
            elif "hello" in query_lower and tool['name'] == 'greet':
                chosen_tools.append({'server_id': tool['server_id'], 'name': tool['name'], 'args': []})
                break
        return chosen_tools
    def _mock_llm_final_answer(self, query: str, tool_results: Dict[str, Any]) -> str:
        if 'get_weather' in tool_results and 'error' not in tool_results['get_weather']:
            weather_data = tool_results['get_weather']
            return (f"The weather in {weather_data['location']} is approximately {weather_data['temperature']}°C "
                    f"({weather_data['description']}) with wind speed {weather_data['windspeed']} m/s.")
        elif 'get_weather' in tool_results and 'error' in tool_results['get_weather']:
            return f"Could not get weather for {tool_results['get_weather']['location']}: {tool_results['get_weather']['error']}"
        elif 'get_customer_count' in tool_results:
            return f"You have {tool_results['get_customer_count']['count']} customers."
        elif 'greet' in tool_results:
            return f"LLM says: {tool_results['greet']['message']}"
        return f"I processed your query: '{query}'. But couldn't form a specific answer from tool results {tool_results}."
    def process_query(self, query: str) -> str:
        available_tools = self._client.discover_tools()
        chosen_tools = self._mock_llm_tool_chooser(query, available_tools)
        if not chosen_tools:
            return self._mock_llm_final_answer(query, {})
        tool_results: Dict[str, Any] = {}
        for tool_selection in chosen_tools:
            server_id = tool_selection['server_id']
            tool_name = tool_selection['name']
            args = tool_selection.get('args', [])
            kwargs = tool_selection.get('kwargs', {})
            try:
                result = self._client.call_tool(server_id, tool_name, *args, **kwargs)
                tool_results[tool_name] = result
            except Exception as e:
                tool_results[tool_name] = {"error": str(e)}
        final_answer = self._mock_llm_final_answer(query, tool_results)
        return final_answer


# 1. Create MCP Servers
data_server = MCPServer(server_id="customer_data_server")
api_server = MCPServer(server_id="external_api_server")
utility_server = MCPServer(server_id="general_utility_server")

# 2. Register tools with their respective servers
data_server.register_tool(
    name="get_customer_count",
    func=_get_customer_database_count,
    description="Fetches the total number of customers from the primary database."
)

api_server.register_tool(
    name="get_weather",
    func=_get_current_weather,
    description="Fetches current weather information for a specified location (e.g., 'London', 'New York')."
)

utility_server.register_tool(
    name="greet",
    func=_greet_user,
    description="Provides a simple greeting."
)

# 3. Create MCP Client and add servers
client = MCPClient()
client.add_server(data_server.server_id, data_server)
client.add_server(api_server.server_id, api_server)
client.add_server(utility_server.server_id, utility_server)

# 4. Create MCP Host
host = MCPHost(client=client)

# 5. Process user queries
print(f"Query 1: 'What's the weather in London?'")
answer1 = host.process_query("What's the weather in London?")
print(f"Answer: {answer1}\n")

print(f"Query 2: 'How many customers do we have?'")
answer2 = host.process_query("How many customers do we have?")
print(f"Answer: {answer2}\n")

print(f"Query 3: 'Hello, AI agent!'")
answer3 = host.process_query("Hello, AI agent!")
print(f"Answer: {answer3}\n")

print(f"Query 4: 'Tell me about the stock market.' (No matching tool)")
answer4 = host.process_query("Tell me about the stock market.")
print(f"Answer: {answer4}\n")

print(f"Query 5: 'What's the weather in Mars?' (Tool exists, but invalid location)")
answer5 = host.process_query("What's the weather in Mars?")
print(f"Answer: {answer5}\n")
```

## What real-world tool this relates to

The `mcp_system` conceptually aligns with several established patterns and frameworks in the AI and data engineering landscape:

*   **LLM Agents / Tools (e.g., LangChain, LlamaIndex)**: This system directly mirrors the "Tool" or "Agent" concept in modern LLM orchestration frameworks. `MCPServer`s are like namespaces for `Tool` definitions, `MCPClient` is the tool executor, and `MCPHost` is the Agent/Orchestrator that selects and uses tools based on an LLM's reasoning.
*   **Function Calling in LLMs**: Modern LLMs (like OpenAI's GPT models or Google's Gemini) support "function calling," where the LLM can generate JSON that describes a function to be called and its arguments. The `mcp_system` provides the backend infrastructure to define these functions (`tools`) and then execute them once the LLM requests it.
*   **Microservices and API Gateways**: Each `MCPServer` can be thought of as a lightweight microservice exposing specific domain-centric functionalities. The `MCPClient` acts similarly to an API Gateway, providing a consolidated access point to these diverse backend services.
*   **Data Virtualization / Federation**: By creating tools that abstract away the underlying data source, the `mcp_system` offers a form of data virtualization, allowing the `MCPClient` and LLM to interact with data in a unified manner regardless of its origin.

## Limitations

This implementation is a conceptual simulation and has certain limitations:

*   **Simulated LLM**: The `_mock_llm_tool_chooser` and `_mock_llm_final_answer` functions are highly simplified heuristics. A production system would integrate with actual LLM APIs (e.g., OpenAI, Anthropic, Google) for robust tool selection and response generation.
*   **In-Process Communication**: All components (`MCPServer`, `MCPClient`, `MCPHost`) currently run within the same Python process. A real-world, scalable MCP would involve network communication (e.g., REST, gRPC) between potentially distributed servers and clients.
*   **No Authentication/Authorization**: The system lacks any security mechanisms for controlling access to tools or data.
*   **Simple Tool Selection Logic**: The mock LLM's tool selection is based on basic keyword matching. Real LLMs employ sophisticated natural language understanding and reasoning to choose and use tools effectively.
*   **Error Handling**: While some basic error handling is present, a production system would require more comprehensive and graceful error management.
```