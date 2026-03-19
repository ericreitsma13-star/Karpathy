import os
import json
import requests
import google.generativeai as genai
from datetime import datetime

# 1. Configuration
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
SUPA_KEY = os.getenv("SUPADATA_API_KEY")

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

def get_supadata(endpoint, params):
    """Helper to call Supadata API"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def scout_market():
    print("🚀 Starting Unwatched Scout...")
    
    # PHASE 1: Generate Search Query
    # Gemini identifies a 2026 tech 'Trap' (e.g. Claude MCP, PQC, GreenOps)
    thinking_prompt = """
    Identify one high-growth technical niche in March 2026 where YouTube tutorials are 
    likely outdated or 'Traps' (e.g. Claude MCP, PQC migration, Next.js 16 server actions).
    Provide ONLY a YouTube search query to find these videos.
    """
    query = model.generate_content(thinking_prompt).text.strip().replace('"', '')
    print(f"🔍 Searching YouTube for: {query}")

    # PHASE 2: Mock Search & Deep Intelligence
    # Since we can't 'search' YouTube directly via Supadata without a URL, 
    # we target a known high-signal 2026 topic if no specific URL is provided.
    # In a full loop, you'd pass a specific URL found via a search API.
    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" # Placeholder
    
    print(f"📊 Pulling Intel for: {target_url}")
    metadata = get_supadata("metadata", {"url": target_url})
    transcript = get_supadata("transcript", {"url": target_url, "text": "true"})

    # PHASE 3: The 'Karpathy' Synthesis
    analysis_prompt = f"""
    Analyze this technical content for the 'Unwatched' App.
    Video Title: {metadata.get('title', 'Unknown')}
    Transcript: {str(transcript.get('content', ''))[:5000]}
    
    Format the output as a Markdown section for 'market_research.md'.
    Include:
    1. The 'Trap' (What is outdated or confusing in this video?)
    2. The 'Fix' (The Skill Block Unwatched should provide).
    3. Signal Score (1-10) based on developer pain.
    """
    
    research_output = model.generate_content(analysis_prompt).text
    
    # PHASE 4: Write to File
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"\n\n---\n## 📈 Trend Analysis: {timestamp}\n"
    
    with open("market_research.md", "a") as f:
        f.write(header)
        f.write(research_output)
    
    print("✅ Market Intelligence Updated.")

if __name__ == "__main__":
    if not GEMINI_KEY or not SUPA_KEY:
        print("❌ Missing API Keys. Check your GitHub Secrets.")
    else:
        scout_market()
