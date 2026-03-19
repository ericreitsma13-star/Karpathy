import os
import time
import requests
from google import genai
from datetime import datetime

# 1. API Setup
# Ensure these are in your GitHub Settings > Secrets > Actions
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

# 2026 Model Selection: Lite for brainstorm, Standard for Deep Analysis
SCOUT_MODEL = 'gemini-2.5-flash-lite'
ANALYST_MODEL = 'gemini-2.5-flash'

def get_supadata(endpoint, params):
    """Fetcher using verified 2026 endpoints and parameters"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    # Respect 1-req/sec rate limit for Supadata Free/Basic
    time.sleep(1.5) 
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Supadata {endpoint} Error {response.status_code}: {response.text}")
        return None
    except Exception as e:
        print(f"❌ Network Error: {e}")
        return None

def scout_market():
    print(f"🚀 Launching Unwatched Analyst ({datetime.now().strftime('%H:%M')})...")
    
    try:
        # PHASE 1: Niche Discovery (Free)
        niche_prompt = (
            "Identify one 2026 tech niche with high 'Knowledge Debt' (outdated YouTube info). "
            "Examples: Claude MCP, PQC implementation, Edge-AI inference. "
            "Return ONLY a specific YouTube search query."
        )
        res = client.models.generate_content(model=SCOUT_MODEL, contents=niche_prompt)
        query = res.text.strip().replace('"', '')
        print(f"🔍 Searching: {query}")

        # PHASE 2: Search (1 Credit)
        # Verified 2026 Parameter: 'q'
        search_res = get_supadata("youtube/search", {"q": query})
        
        # Fallback if niche is too narrow
        if not search_res or not search_res.get("videos"):
            print("🔄 Niche too specific, trying broader 'Developer Traps 2026'...")
            search_res = get_supadata("youtube/search", {"q": "Software Engineering Traps 2026"})

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Failure: No videos found. Check Supadata Dashboard.")
            return

        # PHASE 3: Social Proof Selection
        # Pick the video with the highest commentCount (most user pain)
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing Lead: {top_video['title']} ({top_video.get('commentCount')} comments)")

        # PHASE 4: Extract Intel (1 Credit)
        transcript_data = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_data.get("content", "") if transcript_data else "No transcript available."

        # PHASE 5: Synthesis (Free)
        analysis_prompt = (
            f"Analyze this for the 'Unwatched' App:\n"
            f"Title: {top_video['title']}\n"
            f"Comments: {top_video.get('commentCount')}\n"
            f"Transcript: {str(transcript_text)[:8000]}\n\n"
            "Format: 1. The Trap (Outdated info), 2. The Fix (Modern solution), 3. Signal Score (1-10)."
        )
        report = client.models.generate_content(model=ANALYST_MODEL, contents=analysis_prompt).text

        # PHASE 6: Update the Map
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{report}")
        
        print("✅ Market Intelligence Map Updated.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
