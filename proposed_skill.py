from fastmcp import FastMCP

mcp = FastMCP(name="calculator")

@mcp.tool(args={"a": float, "b": float}, returns=float)
def multiply(a: float, b: float) -> float:
    """Multiplies two numbers."""
    return a * b

@mcp.tool(args={"a": float, "b": float}, returns=float)
def add(a: float, b: float) -> float:
    """Adds two numbers."""
    return a + b
