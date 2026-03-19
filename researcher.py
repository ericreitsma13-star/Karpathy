import os
import time
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.5-flash-lite' 

def get_supadata(endpoint, data_payload):
    """2026 POST-based fetcher for Supadata"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {
        "x-api-key": SUPA_KEY,
        "Content-Type": "application/json"
    }
    
    # 2026 Cooldown
    time.sleep(2) 
    
    # We use .post() with json= to ensure 'query' is sent in the body
    try:
        response = requests.post(url, headers=headers, json=data_payload)
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Supadata {endpoint} Error {response.status_code}: {response.text}")
        return None
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

def scout_market():
    print(f"🚀 Launching Unwatched Analyst (JSON-POST Mode)...")
    
    try:
        # PHASE 1: Niche Discovery (Free)
        res = client.models.generate_content(model=MODEL_ID, contents="YouTube search query for 2026 tech traps.")
        query_text = res.text.strip().replace('"', '')
        print(f"🔍 Brainstormed: {query_text}")

        # PHASE 2: Discovery (JSON Payload)
        search_res = get_supadata("youtube/search", {"query": query_text})
        
        if not search_res or not search_res.get("videos"):
            print("🔄 Retrying with broad query...")
            search_res = get_supadata("youtube/search", {"query": "Software Engineering Traps 2026"})

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Still failing. Check if Supadata endpoint changed to /search/youtube")
            return

        # PHASE 3: Select & Analyze
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']}")

        # PHASE 4: Transcript (Most transcript APIs still use GET/URL)
        # If this fails, we'll switch this to POST too.
        transcript_url = f"https://api.supadata.ai/v1/youtube/transcript"
        transcript_res = requests.get(transcript_url, headers={"x-api-key": SUPA_KEY}, params={"url": video_url, "text": "true"})
        transcript_text = transcript_res.json().get("content", "") if transcript_res.status_code == 200 else ""

        # PHASE 5: Save
        report = client.models.generate_content(model=MODEL_ID, contents=f"Trap/Fix for: {top_video['title']}\n{transcript_text[:5000]}").text
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n{report}")
        
        print("✅ Success! Check market_research.md")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
