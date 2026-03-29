import requests
import json
import uuid # For JSON RPC IDs
import sys

class LLMAdapter:
    """
    A mock LLM adapter to simulate interactions with a Large Language Model.
    In a real scenario, this would use an actual LLM API (e.g., OpenAI, Claude, Gemini).
    """
    def __init__(self, api_key: str = "mock_key"):
        self.api_key = api_key

    def decide_on_resources(self, user_prompt: str, resource_descriptions: list[dict]) -> dict:
        """
        Simulates an LLM deciding which resources are relevant to a user prompt.
        Returns a mock decision indicating a needed resource ID and optional parameters.
        """
        print(f"[LLM Mock] Deciding on resources for prompt: '{user_prompt}'", file=sys.stderr)
        # In a real LLM, this would involve sophisticated prompt engineering or function calling.
        # For simulation, let's apply simple keyword matching.
        
        chosen_resource_id = None
        chosen_parameters = {}

        # Prioritize coffee shops if 'coffee' is explicitly mentioned for location info
        if "coffee" in user_prompt.lower():
            for res_desc in resource_descriptions:
                if "coffee shops" in res_desc['description'].lower():
                    chosen_resource_id = res_desc['id']
                    chosen_parameters = {"query": "coffee near me"}
                    break # Found primary resource, exit loop
        
        # If no coffee shop was selected, or if 'appointment' is the primary intent
        # and related to 'next week', consider calendar.
        # This logic ensures that for a 'coffee appointment', coffee shops data is prioritized for location.
        if chosen_resource_id is None and "appointment" in user_prompt.lower() and "next week" in user_prompt.lower():
            for res_desc in resource_descriptions:
                if "calendar" in res_desc['description'].lower():
                    chosen_resource_id = res_desc['id']
                    chosen_parameters = {"date_range": "next week"}
                    break

        if chosen_resource_id:
            print(f"[LLM Mock] Selected resource: '{chosen_resource_id}' (ID: {chosen_resource_id})", file=sys.stderr)
            return {"needed_resource_id": chosen_resource_id, "parameters": chosen_parameters}
        
        print("[LLM Mock] No specific resource needed for this prompt (mock logic).", file=sys.stderr)
        return {"needed_resource_id": None, "parameters": {}}

    def decide_on_tool_invocation(self, user_prompt: str, resource_data: str, tool_descriptions: list[dict]) -> dict:
        """
        Simulates an LLM deciding which tool to invoke based on prompt and resource data.
        Tool descriptions are typically passed as structured data to the LLM API.
        Returns a mock tool invocation recommendation.
        """
        print(f"[LLM Mock] Deciding on tool for prompt: '{user_prompt}' (Resource data: '{resource_data[:30]}...')", file=sys.stderr)

        # Simulate picking 'calendar_invite_tool' if prompt mentions 'next week' and 'appointment'.
        if "next week" in user_prompt.lower() and ("appointment" in user_prompt.lower() or "meet" in user_prompt.lower()):
            for tool_desc in tool_descriptions:
                if tool_desc['name'] == 'Create Calendar Invite': # Matching based on name as per mock server
                    print(f"[LLM Mock] Recommended tool: '{tool_desc['name']}' (ID: {tool_desc['id']})", file=sys.stderr)
                    return {
                        "invoke_tool_id": tool_desc['id'],
                        "parameters": {
                            "attendee": "Peter", # Extracted from user_prompt
                            "topic": "Coffee meeting",
                            "time_suggestion": "next week",
                            "location_info": resource_data # Relevant resource data passed
                        }
                    }
        print("[LLM Mock] No specific tool invocation needed (mock logic).", file=sys.stderr)
        return {"invoke_tool_id": None, "parameters": {}}


class MCPClient:
    """
    Implements the client-side of the Model Context Protocol (MCP).
    This client interacts with an MCP server to discover capabilities,
    retrieve resources, and ultimately facilitate tool invocation
    driven by an LLM.
    
    Communication uses JSON RPC over HTTP POST to well-known RESTful endpoints.
    """
    def __init__(self, server_url: str):
        if not server_url.startswith("http"): # Basic URL validation
            raise ValueError("Server URL must start with http or https.")
        self.server_url = server_url.rstrip('/') # Ensure no trailing slash
        self.session = requests.Session() # Use a session for persistent connections

    def _make_jsonrpc_request(self, endpoint: str, method: str, params: dict = None) -> dict:
        """
        Sends a JSON RPC request to the MCP server at a specific endpoint.
        """
        rpc_request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params if params is not None else {},
            "id": str(uuid.uuid4()) # Unique request ID
        }
        full_url = f"{self.server_url}{endpoint}"
        print(f"[MCP Client] Sending JSON RPC request to {full_url} (Method: {method})..", file=sys.stderr)
        try:
            response = self.session.post(full_url, json=rpc_request, headers={"Content-Type": "application/json"})
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            json_response = response.json()
            if "error" in json_response:
                raise RuntimeError(f"MCP Server returned an error: {json_response['error']}")
            return json_response.get("result", {})
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to connect to MCP server at {full_url}: {e}") from e
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON response from MCP server at {full_url}: {response.text}")

    def get_capabilities(self) -> dict:
        """
        Retrieves the server's capabilities, including descriptions of
        available tools, resources, and prompts.
        Corresponds to the '/capabilities' RESTful endpoint with a 'get_capabilities' JSON RPC method.
        """
        return self._make_jsonrpc_request(endpoint="/capabilities", method="get_capabilities")

    def get_resources_descriptions(self) -> list[dict]:
        """
        Retrieves a list of available resources with their descriptions from the server's capabilities.
        """
        capabilities = self.get_capabilities()
        return capabilities.get("resources", [])

    def get_tools_descriptions(self) -> list[dict]:
        """
        Retrieves a list of available tools with their descriptions and schemas from the server's capabilities.
        """
        capabilities = self.get_capabilities()
        return capabilities.get("tools", [])

    def get_resource_data(self, resource_id: str, parameters: dict = None) -> str:
        """
        Retrieves the actual data for a specific resource, potentially
        passing parameters for filtering or selection.
        Corresponds to the '/resources' RESTful endpoint with a 'get_resource_data' JSON RPC method.
        """
        print(f"[MCP Client] Requesting data for resource ID '{resource_id}' with params {parameters or {}}..", file=sys.stderr)
        return self._make_jsonrpc_request(
            endpoint="/resources", # This endpoint could serve multiple resource-related methods
            method="get_resource_data",
            params={"resource_id": resource_id, "parameters": parameters or {}}
        )

    def invoke_tool(self, tool_id: str, parameters: dict) -> dict:
        """
        Simulates invoking a tool as recommended by the LLM.
        In a real scenario, this would trigger the actual tool's action
        (e.g., API call to a calendar service, database update, etc.)
        This method represents the client's execution of the LLM's recommendation,
        not another MCP server call.
        """
        print(f"[MCP Client] Invoking tool ID '{tool_id}' with parameters: {parameters}", file=sys.stderr)
        # For this example, we'll just return a mock success status.
        # A real implementation would involve specific code to interact with the target system/API.
        print(f"[MCP Client] Tool '{tool_id}' executed successfully with params {parameters}. (Mock Action)", file=sys.stderr)
        return {"status": "success", "tool_id": tool_id, "parameters": parameters, "mock_result": "Action completed"}

    def run_agentic_workflow(self, user_prompt: str, llm_adapter: LLMAdapter) -> dict:
        """
        Orchestrates the agentic workflow as described in the Model Context Protocol:
        1. The host application (client) gets capabilities (resources, tools) from the MCP server.
        2. The host application asks the LLM to identify relevant resources based on the user prompt.
        3. If resources are needed, the host application fetches their data from the MCP server.
        4. The host application then asks the LLM to recommend a tool invocation,
           providing user prompt, resource data, and tool descriptions.
        5. The host application invokes the recommended tool.
        """
        print(f"\n--- Starting MCP Agentic Workflow for prompt: '{user_prompt}' ---", file=sys.stderr)

        # 1. Get resources and tools descriptions from MCP Server
        resource_descriptions = self.get_resources_descriptions()
        tool_descriptions = self.get_tools_descriptions()
        print(f"[MCP Client] Discovered {len(resource_descriptions)} resources and {len(tool_descriptions)} tools from server.", file=sys.stderr)

        # 2. Ask LLM to identify relevant resources
        llm_resource_decision = llm_adapter.decide_on_resources(user_prompt, resource_descriptions)
        needed_resource_id = llm_resource_decision.get("needed_resource_id")
        resource_parameters = llm_resource_decision.get("parameters", {})

        resource_data = ""
        if needed_resource_id:
            print(f"[MCP Client] LLM recommended resource '{needed_resource_id}'. Fetching data...", file=sys.stderr)
            resource_data = self.get_resource_data(needed_resource_id, resource_parameters)
            print(f"[MCP Client] Received resource data (first 50 chars): '{resource_data[:50]}...'", file=sys.stderr)
        else:
            print("[MCP Client] LLM did not recommend any specific resources.", file=sys.stderr)

        # 3. Ask LLM to recommend a tool invocation
        llm_tool_decision = llm_adapter.decide_on_tool_invocation(user_prompt, resource_data, tool_descriptions)
        tool_to_invoke_id = llm_tool_decision.get("invoke_tool_id")
        tool_invocation_parameters = llm_tool_decision.get("parameters", {})

        invocation_result = None
        workflow_reason = None # Initialize reason
        if tool_to_invoke_id:
            print(f"[MCP Client] LLM recommended tool '{tool_to_invoke_id}'. Invoking...", file=sys.stderr)
            invocation_result = self.invoke_tool(tool_to_invoke_id, tool_invocation_parameters)
            print(f"[MCP Client] Tool invocation result: {invocation_result}", file=sys.stderr)
        else:
            print("[MCP Client] LLM did not recommend any specific tool invocation.", file=sys.stderr)
            workflow_reason = "No tool recommended" # Set reason for the test case

        print(f"--- MCP Agentic Workflow complete ---\n", file=sys.stderr)
        return {"status": "workflow_complete", "final_tool_invocation": invocation_result, "reason": workflow_reason}