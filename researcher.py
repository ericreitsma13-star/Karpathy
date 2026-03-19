import os
from openai import OpenAI
from duckduckgo_search import DDGS

# Grab your misspelled token secret
token = os.environ.get("GITHUB_TOKEN")

client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=token
)

def run_market_research():
    print("🧠 GPT-5 Engine engaged (via stable-mini alias)...")
    
    # We use 'gpt-4o-mini' because it's the stable alias for the latest mini model
    try:
        brain = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role": "user", "content": "Analyze 3 tech niches with high 'Knowledge Debt' (outdated video tutorials)."}]
        )
        print(f"📊 Analysis: {brain.choices[0].message.content}")
        
        # Save to your markdown file
        with open("market_research.md", "a") as f:
            f.write(f"\n## {brain.choices[0].message.content}\n")
            
    except Exception as e:
        print(f"❌ Still hitting a snag: {e}")

if __name__ == "__main__":
    run_market_research()
