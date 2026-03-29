import sys
from proposed_skill import *

import pytest
import http.server
import socketserver
import threading
import json
import time
import sys

# Mock MCP Server Handler
class MockMCPHandler(http.server.BaseHTTPRequestHandler):
    """
    A simple handler for our mock MCP server.
    Responds to JSON RPC requests for capabilities, resources, and resource data.
    """
    def _send_response(self, data, status_code=200):
        self.send_response(status_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        try:
            request_data = json.loads(post_data.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_response({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None}, 400)
            return

        method = request_data.get("method")
        params = request_data.get("params", {})
        request_id = request_data.get("id")

        result = None
        error = None

        if self.path == "/capabilities":
            if method == "get_capabilities":
                result = {
                    "resources": [
                        {"id": "calendar", "name": "User Calendar", "description": "Access to user's free/busy calendar slots."},
                        {"id": "coffee_shops", "name": "Local Coffee Shops", "description": "List of coffee shops in the area, powered by Yelp."},
                        {"id": "restaurants", "name": "Local Restaurants", "description": "List of restaurants for dinner."}
                    ],
                    "tools": [
                        {"id": "calendar_invite_tool", "name": "Create Calendar Invite", "description": "Creates a new calendar event.", "schema": {"type": "object", "properties": {"attendee": {"type": "string"}, "topic": {"type": "string"}, "time_suggestion": {"type": "string"}, "location_info": {"type": "string"}}}}}, 
                        {"id": "restaurant_reservation_tool", "name": "Make Restaurant Reservation", "description": "Books a table at a restaurant.", "schema": {"type": "object", "properties": {"restaurant_name": {"type": "string"}, "time": {"type": "string"}, "guests": {"type": "integer"}}}}
                    ]
                }
            else:
                error = {"code": -32601, "message": "Method not found: capabilities"}
        elif self.path == "/resources":
            if method == "get_resource_data":
                resource_id = params.get("resource_id")
                if resource_id == "coffee_shops":
                    result = f"Starbucks, Peet's Coffee, Blue Bottle Coffee. Query: {params.get('parameters', {}).get('query', 'N/A')}"
                elif resource_id == "calendar":
                    result = f"Calendar free slots for {params.get('parameters', {}).get('date_range', 'N/A')}: Mon 9-12, Tue 1-5"
                else:
                    error = {"code": -32000, "message": f"Resource '{resource_id}' not found"}
            else:
                error = {"code": -32601, "message": "Method not found: resources"}
        else:
            error = {"code": -32601, "message": "Endpoint not found"}

        response_payload = {"jsonrpc": "2.0", "id": request_id}
        if error:
            response_payload["error"] = error
            self._send_response(response_payload, 500)
        else:
            response_payload["result"] = result
            self._send_response(response_payload)

class MockMCPServer(socketserver.TCPServer):
    allow_reuse_address = True # Allow immediate reuse of the port

def run_mock_server_in_thread(port):
    server = MockMCPServer(("", port), MockMCPHandler) # Fixed: Changed pesas to ("", port)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True # Allow main thread to exit even if server thread is running
    server_thread.start()
    print(f"Mock MCP Server started on port {port} in background.", file=sys.stderr)
    return server # Return server object so it can be shut down


@pytest.fixture(scope="module")
def mcp_server_url():
    port = 8000 # Use a fixed port for the mock server
    server = run_mock_server_in_thread(port)
    # Give the server a moment to start up
    time.sleep(0.1)
    yield f"http://localhost:{port}"
    server.shutdown()
    server.server_close()
    print(f"Mock MCP Server on port {port} shut down.", file=sys.stderr)


# Test cases for MCPClient
def test_mcp_client_init():
    client = MCPClient("http://localhost:8000")
    assert client.server_url == "http://localhost:8000"
    with pytest.raises(ValueError, match="Server URL must start with http or https."):
        MCPClient("badurl")

def test_get_capabilities(mcp_server_url):
    client = MCPClient(mcp_server_url)
    capabilities = client.get_capabilities()
    assert "resources" in capabilities
    assert isinstance(capabilities["resources"], list)
    assert len(capabilities["resources"]) > 0
    assert "tools" in capabilities
    assert isinstance(capabilities["tools"], list)
    assert len(capabilities["tools"]) > 0
    assert any(r['id'] == 'coffee_shops' for r in capabilities['resources'])
    assert any(t['id'] == 'calendar_invite_tool' for t in capabilities['tools'])

def test_get_resources_descriptions(mcp_server_url):
    client = MCPClient(mcp_server_url)
    resources = client.get_resources_descriptions()
    assert isinstance(resources, list)
    assert len(resources) > 0
    assert any(r['name'] == 'Local Coffee Shops' for r in resources)

def test_get_tools_descriptions(mcp_server_url):
    client = MCPClient(mcp_server_url)
    tools = client.get_tools_descriptions()
    assert isinstance(tools, list)
    assert len(tools) > 0
    assert any(t['name'] == 'Create Calendar Invite' for t in tools)

def test_get_resource_data(mcp_server_url):
    client = MCPClient(mcp_server_url)
    coffee_data = client.get_resource_data("coffee_shops", {"query": "best rated"})
    assert "Starbucks" in coffee_data
    assert "Query: best rated" in coffee_data

    calendar_data = client.get_resource_data("calendar", {"date_range": "today"})
    assert "Calendar free slots" in calendar_data
    assert "today" in calendar_data

    with pytest.raises(RuntimeError, match="Resource 'nonexistent' not found"):
        client.get_resource_data("nonexistent")

def test_invoke_tool():
    client = MCPClient("http://localhost:8000") # URL doesn't matter for this mocked method
    tool_id = "mock_tool_123"
    params = {"arg1": "value1"}
    result = client.invoke_tool(tool_id, params)
    assert result == {"status": "success", "tool_id": tool_id, "parameters": params, "mock_result": "Action completed"}


# Test cases for the full agentic workflow
def test_run_agentic_workflow_coffee_appointment(mcp_server_url):
    client = MCPClient(mcp_server_url)
    llm_adapter = LLMAdapter()
    user_prompt = "I want to meet Peter for coffee next week for an appointment."

    workflow_result = client.run_agentic_workflow(user_prompt, llm_adapter)

    assert workflow_result["status"] == "workflow_complete"
    assert workflow_result["final_tool_invocation"] is not None
    assert workflow_result["final_tool_invocation"]["tool_id"] == "calendar_invite_tool"
    assert "Coffee meeting" in workflow_result["final_tool_invocation"]["parameters"]["topic"]
    # This assertion expects coffee shop data, which now comes from prioritizing 'coffee_shops' resource.
    assert "Starbucks" in workflow_result["final_tool_invocation"]["parameters"]["location_info"]

def test_run_agentic_workflow_no_tool_needed(mcp_server_url):
    client = MCPClient(mcp_server_url)
    llm_adapter = LLMAdapter()
    user_prompt = "Tell me a fun fact about Python."

    workflow_result = client.run_agentic_workflow(user_prompt, llm_adapter)

    assert workflow_result["status"] == "workflow_complete"
    assert workflow_result["final_tool_invocation"] is None
    assert "No tool recommended" in workflow_result["reason"]
