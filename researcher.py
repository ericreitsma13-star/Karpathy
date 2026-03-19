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
    print("🚀 Starting Unwatched Scout (Syntax Fixed)...")
    
    # PHASE 1: Generate High-Pain Niche
    thinking_prompt = "Identify a technical niche in March 2026 with outdated YouTube content. Provide ONLY a search query."
    
    # FIXED: Corrected call structure
    response = client.models.generate_content(
        model='gemini-2.0-flash-001', 
        contents=thinking_prompt
    )
    query = response.text.strip().replace('"', '')
    print(f"🔍 Researching Niche: {query}")

    # PHASE 2: Intelligence & Social Proof
    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" # Placeholder
    metadata = get_supadata("metadata", {"url": target_url})
    
    comment_count = int(metadata.get('commentCount', 0))
    print(f"📊 Social Check: {comment_count} comments.")
    
    if comment_count < 1: # Set to 1 for testing, increase to 10 for production
        print("⏭️ Skipping: Low social signal.")
        return

    # PHASE 3: Transcript & Synthesis
    transcript = get_supadata("transcript", {"url": target_url, "text": "true"})
    
    # FIXED: Ensured no trailing/extra brackets in the prompt f-string
    analysis_prompt = (
        f"Video Title: {metadata.get('title')}\n"
        f"Comments: {comment_count}\n"
        f"Transcript Snippet: {str(transcript.get('content', ''))[:5000]}\n\n"
        "Format as Markdown: \n"
        "1. The Trap\n2. The Fix\n3. Social Signal\n4. Signal Score (1-10)"
    )
    
    # FIXED: Proper closing for the final API call
    research_output = client.models.generate_content(
        model='gemini-2.0-flash-001',
        contents=analysis_prompt
    ).text
    
    # PHASE 4: Update the Intelligence Map
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {metadata.get('title')}\n")
        f.write(research_output)
    
    print("✅ Market Intelligence Map Updated.")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY.")
    else:
        scout_market()
