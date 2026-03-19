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
    """The 2026 'Truth' Fetcher"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    time.sleep(2) 
    
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"📡 API Call: {endpoint} | Status: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Error: {response.text}")
        return None
    except Exception as e:
        print(f"❌ Network Error: {e}")
        return None

def scout_market():
    print(f"🚀 Launching Unwatched Analyst (Debug Mode)...")
    
    try:
        # PHASE 1: NICHE (Free)
        # We're using a query that is GUARANTEED to have videos in 2026
        query_text = "Cursor AI coding agent tutorial 2026"
        
        # PHASE 2: SEARCH (1 Credit)
        # We know /v1/search + 'q' works because of your credit jump
        search_res = get_supadata("search", {"q": query_text, "type": "youtube"})
        
        if not search_res or "videos" not in search_res or not search_res["videos"]:
            print("❌ Search returned 0 videos. Raw Response:", search_res)
            return

        # PHASE 3: SELECT
        videos = search_res["videos"]
        # Filter for videos that actually have comments
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Targeted: {top_video['title']} ({top_video.get('commentCount')} comments)")

        # PHASE 4: TRANSCRIPT (2 Credits)
        t_res = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = t_res.get("content", "") if t_res else ""

        # PHASE 5: AI ANALYSIS (Free)
        analysis = client.models.generate_content(
            model=MODEL_ID, 
            contents=f"Identify Trap/Fix for: {top_video['title']}\n{str(transcript_text)[:5000]}"
        ).text

        # PHASE 6: THE SAVE (The most important part)
        with open("market_research.md", "a", encoding="utf-8") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d %H:%M')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{analysis}")
        
        print("✅ SUCCESS! Intelligence Map Updated.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
