import os
import time
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.5-flash-lite' 

def get_supadata(endpoint, params):
    """Fetcher using the EXACT parameter 'query' requested by the 400 error"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    # 2026 Rate Limit Cooldown
    time.sleep(2) 
    
    # We are switching to 'query' as the key because the Error 400 explicitly asked for it
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"⚠️ Supadata {endpoint} Error {response.status_code}: {response.text}")
        return None
    return response.json()

def scout_market():
    print(f"🚀 Launching Unwatched Analyst (Force-Query Mode)...")
    
    try:
        # PHASE 1: Niche Discovery
        niche_prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
        res = client.models.generate_content(model=MODEL_ID, contents=niche_prompt)
        query_text = res.text.strip().replace('"', '')
        print(f"🔍 Brainstormed: {query_text}")

        # PHASE 2: Discovery
        # THE FIX: The error said 'query: Required'. So we use 'query'.
        search_res = get_supadata("youtube/search", {"query": query_text})
        
        if not search_res or not search_res.get("videos"):
            print("🔄 Niche too specific, trying fallback...")
            search_res = get_supadata("youtube/search", {"query": "Software Engineering Traps 2026"})

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Absolute failure. Check credits at supadata.ai/dashboard")
            return

        # PHASE 3: Selection
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']}")

        # PHASE 4: Transcript (Uses 'url' parameter, which we know works)
        transcript_data = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_data.get("content", "") if transcript_data else ""

        # PHASE 5: Synthesis
        analysis_prompt = f"Identify Trap/Fix for: {top_video['title']}\nTranscript: {str(transcript_text)[:5000]}"
        report = client.models.generate_content(model=MODEL_ID, contents=analysis_prompt).text

        # PHASE 6: Save
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{report}")
        
        print("✅ Success. Market Intelligence Map Updated.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
