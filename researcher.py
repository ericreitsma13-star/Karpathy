import os
import requests
from google import genai
from datetime import datetime

# 1. Client Setup
# The 2026 SDK automatically reads GEMINI_API_KEY from your environment
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
    print("🚀 Starting Unwatched Scout (Social Proof Enabled)...")
    
    # PHASE 1: Generate High-Pain Niche
    thinking_prompt = """
    Identify one technical niche in March 2026 where YouTube tutorials are failing 
    (e.g., Model Context Protocol, PQC Migration, GreenOps energy labels).
    Provide ONLY a YouTube search query to find these 'Trap' videos.
    """
    response = client.models.generate_content(
        model='gemini-2.0-flash', 
        contents=thinking_prompt
    )
    query = response.text.strip().replace('"', '')
    print(f"🔍 Researching Niche: {query}")

    # PHASE 2: Intelligence & Social Proof
    # In a full loop, you'd use a Search API here. Using placeholder for the 'Trap' video.
    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" 
    
    metadata = get_supadata("metadata", {"url": target_url})
    
    # --- SOCIAL PROOF CHECK ---
    comment_count = int(metadata.get('commentCount', 0))
    view_count = int(metadata.get('viewCount', 0))
    
    print(f"📊 Social Check: {view_count} views, {comment_count} comments.")
    
    if comment_count < 10:
        print("⏭️ Skipping: Low social signal. We need active 'pain' in the comments.")
        return

    # PHASE 3: Transcript & Synthesis
    transcript = get_supadata("transcript", {"url": target_url, "text": "true"})
    
    analysis_prompt = f"""
    Video Title: {metadata.get('title')}
    Comments: {comment_count}
    Transcript: {str(transcript.get('content', ''))[:5000]}
    
    Format as Markdown for 'market_research.md':
    1. The 'Trap': What is specifically outdated/broken in this video?
    2. The 'Fix': The specific Skill Block 'Unwatched' provides.
    3. Social Signal: Why are these {comment_count} people a good audience?
    4. Signal Score (1-10).
    """
    
    research_output = client.models.generate_content(
        model='gemini-2.0-flash',
        contents=analysis_prompt
    ).text
    
    # PHASE 4: Update the Intelligence Map
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {metadata.get('title')}\n")
        f.write(research_output)
    
    print("✅ Market Intelligence Map Updated with Social Proof.")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY in environment.")
    else:
        scout_market()
