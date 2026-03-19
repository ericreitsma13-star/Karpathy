import os
from openai import OpenAI
from duckduckgo_search import DDGS

token = os.environ.get("GITHUB_TOKEN")
client = OpenAI(base_url="https://models.inference.ai.azure.com", api_key=token)

def run_market_research():
    print("🧠 GPT-5 is analyzing Market Knowledge Debt...")
    
    # Phase A: Brainstorming High-Pain Technical Verticals
    analysis_prompt = """
    Analyze the current developer ecosystem for 'Knowledge Debt'. 
    Find 3 distinct audience clusters currently struggling with outdated video tutorials.
    For each, provide:
    1. Audience Name (e.g., 'The AI-Wrapper Builders')
    2. Primary Frustration (e.g., 'API breaking changes since Oct 2025')
    3. Search Query to validate the depth of this pain.
    """
    
    brain = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role": "user", "content": analysis_prompt}]
    )
    
    market_map = brain.choices[0].message.content
    print(f"📊 Market Map: \n{market_map}")

    # Phase B: Validation Search
    # We take the first query the AI suggested and check the 'pulse' on Reddit
    first_query = market_map.split("Query:")[1].split("\n")[0].strip().replace('"', '')
    
    with DDGS() as ddgs:
        samples = [r for r in ddgs.text(f"{first_query} site:reddit.com", max_results=5)]

    # Phase C: Save to Research Log
    with open("market_research.md", "a") as f:
        f.write(f"\n## Research Date: {os.popen('date').read()}\n")
        f.write(f"### Strategy:\n{market_map}\n")
        f.write("### Validation Links:\n")
        for s in samples:
            f.write(f"- [{s['title']}]({s['href']})\n")

if __name__ == "__main__":
    run_market_research()
