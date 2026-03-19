import os
import requests
from google import genai
from datetime import datetime

# 1. Client Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ Supadata Error: {e}")
        return {}

def scout_market():
    print("🚀 Starting Unwatched Scout (Emergency Model Bypass)...")
    
    # PHASE 1: Generate High-Pain Niche
    thinking_prompt = "Identify a technical niche in 2026 with outdated YouTube content. Provide ONLY a search query."
    
    # SWITCHED: Using 1.5-flash to bypass the 2.0 Quota Lock
    try:
        response = client.models.generate_content(
            model='gemini-1.5-flash', 
            contents=thinking_prompt
        )
    except Exception as e:
        print(f"❌ Gemini Error: {e}")
        return

    query = response.text.strip().replace('"', '')
    print(f"🔍 Researching Niche: {query}")

    # PHASE 2: Intelligence & Social Proof
    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" 
    metadata = get_supadata("metadata", {"url": target_url})
    
    comment_count = int(metadata.get('commentCount', 0))
    print(f"📊 Social Check: {comment_count} comments.")
    
    # PHASE 3: Transcript & Synthesis
    transcript = get_supadata("transcript", {"url": target_url, "text": "true"})
    
    analysis_prompt = (
        f"Analyze for Unwatched App:\n"
        f"Title: {metadata.get('title')}\n"
        f"Transcript: {str(transcript.get('content', ''))[:5000]}\n\n"
        "Output as Markdown: Trap, Fix, Signal Score."
    )
    
    # SWITCHED: Using 1.5-flash here too
    research_output = client.models.generate_content(
        model='gemini-1.5-flash',
        contents=analysis_prompt
    ).text
    
    # PHASE 4: Update the Intelligence Map
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {metadata.get('title')}\n")
        f.write(research_output)
    
    print("✅ Market Intelligence Map Updated via Gemini 1.5 Bypass.")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY.")
    else:
        scout_market()
