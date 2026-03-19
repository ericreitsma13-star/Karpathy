import os
from openai import OpenAI
from duckduckgo_search import DDGS # No API Key needed

# 1. Setup GPT-5
token = os.environ.get("GITHUB_TOKEN")
client = OpenAI(base_url="https://models.inference.ai.azure.com", api_key=token)

def run_unwatched_scout():
    print("🚀 GPT-5 is brainstorming 'Knowledge Debt'...")
    
    # Phase A: Brainstorming the 'Pain'
    brainstorm = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[{"role": "user", "content": "Generate a search query to find recent Reddit threads where users are frustrated by an outdated or broken coding tutorial."}]
    )
    query = brainstorm.choices[0].message.content.strip().replace('"', '')
    print(f"🔎 Searching DuckDuckGo for: {query}")

    # Phase B: Free Search (DuckDuckGo)
    with DDGS() as ddgs:
        results = [r for r in ddgs.text(f"{query} site:reddit.com", max_results=5)]

    # Phase C: Save to CSV
    with open("leads.csv", "a") as f:
        for r in results:
            f.write(f"{r['title']} | {r['href']}\n")
            print(f"✅ Found Lead: {r['title']}")

if __name__ == "__main__":
    run_unwatched_scout()
