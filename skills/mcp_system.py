"""
Skill: mcp_system
Source: https://youtube.com/watch?v=eur8dUO9mvE
Title: What is MCP? Integrate AI Agents with Databases & APIs
Added: 2026-03-24
"""

import requests
import json
import time
from typing import Dict, Any, List, Callable, Tuple

class MCPServer:
    """
    Simulates an MCP Server component.
    Manages and executes tools that can connect to data sources or APIs.
    """
    def __init__(self, server_id: str):
        self.server_id = server_id
        self._tools: Dict[str, Tuple[Callable, str]] = {}

    def register_tool(self, name: str, func: Callable, description: str):
        """
        Registers a tool with the server.
        :param name: Unique name for the tool.
        :param func: The callable function that implements the tool's logic.
        :param description: A brief description of what the tool does.
        """
        if name in self._tools:
            raise ValueError(f"Tool '{name}' already registered.")
        self._tools[name] = (func, description)

    def get_available_tools(self) -> List[Dict[str, str]]:
        """
        Returns a list of available tools on this server, including their descriptions.
        """
        return [{"name": name, "description": desc} for name, (_, desc) in self._tools.items()]

    def execute_tool(self, name: str, *args, **kwargs) -> Any:
        """
        Executes a registered tool.
        :param name: The name of the tool to execute.
        :param args: Positional arguments to pass to the tool's function.
        :param kwargs: Keyword arguments to pass to the tool's function.
        """
        if name not in self._tools:
            raise ValueError(f"Tool '{name}' not found on server '{self.server_id}'.")
        func, _ = self._tools[name]
        # print(f"Server '{self.server_id}' executing tool '{name}' with args: {args}, kwargs: {kwargs}")
        return func(*args, **kwargs)

class MCPClient:
    """
    Simulates an MCP Client component.
    Connects to multiple MCP Servers to discover and call tools.
    """
    def __init__(self):
        self._servers: Dict[str, MCPServer] = {}

    def add_server(self, server_id: str, server_instance: MCPServer):
        """
        Registers an MCP Server instance with this client.
        :param server_id: A unique identifier for the server.
        :param server_instance: An instance of MCPServer.
        """
        if server_id in self._servers:
            raise ValueError(f"Server '{server_id}' already registered.")
        self._servers[server_id] = server_instance
        # print(f"Client registered server: {server_id}")

    def discover_tools(self) -> List[Dict[str, Any]]:
        """
        Queries all registered servers for their available tools.
        Returns a consolidated list of tools, indicating which server each tool belongs to.
        """
        all_tools = []
        for server_id, server in self._servers.items():
            for tool_info in server.get_available_tools():
                tool_info['server_id'] = server_id
                all_tools.append(tool_info)
        return all_tools

    def call_tool(self, server_id: str, tool_name: str, *args, **kwargs) -> Any:
        """
        Calls a specific tool on a specific server.
        :param server_id: The ID of the server where the tool is registered.
        :param tool_name: The name of the tool to execute.
        :param args: Positional arguments for the tool.
        :param kwargs: Keyword arguments for the tool.
        """
        server = self._servers.get(server_id)
        if not server:
            raise ValueError(f"Server '{server_id}' not found.")
        return server.execute_tool(tool_name, *args, **kwargs)

class MCPHost:
    """
    Simulates an MCP Host application (e.g., chat app, IDE assistant).
    Orchestrates interaction between a user query, MCP Client, and a Large Language Model.
    """
    def __init__(self, client: MCPClient):
        self._client = client
        # print("MCP Host initialized with client.")

    def _mock_llm_tool_chooser(self, query: str, available_tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Mocks an LLM's decision-making process for choosing tools.
        In a real scenario, this would be an actual LLM call.
        Returns a list of tools (with server_id, name, and chosen_args) to execute.
        """
        # print(f"Host: Mock LLM received query: '{query}' and available tools: {available_tools}")
        chosen_tools = []
        
        # Simple heuristic: if query contains keywords, choose corresponding tool.
        # This part is highly simplified for demonstration.
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
        
        # print(f"Host: Mock LLM chose tools: {chosen_tools}")
        return chosen_tools

    def _mock_llm_final_answer(self, query: str, tool_results: Dict[str, Any]) -> str:
        """
        Mocks an LLM's process of synthesizing a final answer from tool results.
        """
        # print(f"Host: Mock LLM synthesizing final answer for query: '{query}' with tool results: {tool_results}")
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
        """
        Processes a user query by orchestrating MCP client interactions and LLM calls.
        """
        # print(f"\nHost: Received user query: '{query}'")

        # 1. Host/Client needs to retrieve tools from MCP server
        available_tools = self._client.discover_tools()
        # print(f"Host: Discovered available tools: {available_tools}")

        # 2. Host connects to LLM, sends question + available tools
        #    LLM replies with which tools to use
        chosen_tools = self._mock_llm_tool_chooser(query, available_tools)
        if not chosen_tools:
            return self._mock_llm_final_answer(query, {}) # LLM couldn't find a tool

        # 3. Host/Client calls MCP servers to get tool results
        tool_results: Dict[str, Any] = {}
        for tool_selection in chosen_tools:
            server_id = tool_selection['server_id']
            tool_name = tool_selection['name']
            args = tool_selection.get('args', [])
            kwargs = tool_selection.get('kwargs', {})
            try:
                result = self._client.call_tool(server_id, tool_name, *args, **kwargs)
                tool_results[tool_name] = result
                # print(f"Host: Got result for tool '{tool_name}': {result}")
            except Exception as e:
                tool_results[tool_name] = {"error": str(e)}
                # print(f"Host: Error executing tool '{tool_name}': {e}")

        # 4. Host sends tool results back to LLM
        # 5. LLM provides final answer
        final_answer = self._mock_llm_final_answer(query, tool_results)
        # print(f"Host: Final Answer: '{final_answer}'")
        return final_answer

# Dummy tool implementations using stdlib and requests
def _get_current_weather(location: str) -> Dict[str, Any]:
    """Simulates fetching weather data from an external API using requests.
    Uses Open-Meteo for demonstration (no API key needed for basic usage).
    """
    # print(f"Tool: Fetching weather for {location} using simulated API...")
    
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
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        data = response.json()['current_weather']
        return {
            "location": location,
            "temperature": data['temperature'],
            "description": f"Weather code {data['weathercode']}", 
            "windspeed": data['windspeed']
        }
    except requests.exceptions.Timeout:
        return {"location": location, "error": "API request timed out."}
    except requests.exceptions.RequestException as e:
        return {"location": location, "error": f"API request failed: {e}"}
    except json.JSONDecodeError as e:
        return {"location": location, "error": f"API response JSON decode failed: {e}"}
    except KeyError:
        return {"location": location, "error": "API response missing 'current_weather' data."}

def _get_customer_database_count() -> Dict[str, int]:
    """Simulates querying a database for customer count.
    In a real scenario, this would connect to a DB (e.g., psycopg2, sqlalchemy).
    """
    # print("Tool: Querying database for customer count...")
    time.sleep(0.1) # Simulate database latency
    return {"count": 12345}

def _greet_user() -> Dict[str, str]:
    """A simple tool to greet the user.
    """
    # print("Tool: Greeting user...")
    return {"message": "Hello from the MCP server!"}
