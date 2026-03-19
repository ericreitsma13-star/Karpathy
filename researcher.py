import os
import json
import requests
from google import genai
from datetime import datetime

# 1. Setup Clients
# The new SDK automatically looks for GEMINI_API_KEY in your env
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def scout_market():
    print("🚀 Starting Unwatched Scout (2026 SDK)...")
    
    # PHASE 1: Generate Search Query
    # Note: Method changed from 'generate_content' to 'models.generate_content'
    thinking_prompt = "Identify a 2026 tech 'Trap' niche. Provide ONLY a YouTube search query."
    response = client.models.generate_content(
        model='gemini-2.0-flash', # Or gemini-2.5-flash
        contents=thinking_prompt
    )
    query = response.text.strip().replace('"', '')
    print(f"🔍 Searching: {query}")

    # PHASE 2: Intelligence (Using a known high-signal URL for the loop)
    # In your full version, you'd use a Search API to find the URL first
    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" 
    
    metadata = get_supadata("metadata", {"url": target_url})
    transcript = get_supadata("transcript", {"url": target_url, "text": "true"})

    # PHASE 3: Synthesis
    analysis_prompt = f"""
    Analyze for 'Unwatched' App:
    Title: {metadata.get('title', 'Unknown')}
    Transcript: {str(transcript.get('content', ''))[:5000]}
    
    Format as Markdown: Trap, Fix, and Signal Score (1-10).
    """
    
    research_output = client.models.generate_content(
        model='gemini-2.0-flash',
        contents=analysis_prompt
    ).text
    
    # PHASE 4: Save Result
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write(research_output)
    
    print("✅ Market Intelligence Updated.")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY.")
    else:
        scout_market()
