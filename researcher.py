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
    """The 2026 Verified GET Fetcher - Path: /v1/youtube/search | Param: text"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    # Supadata Free Tier: 1 request per second. We wait 2.5s to be ultra safe.
    time.sleep(2.5) 
    
    try:
        # 'text' is the parameter that finally moved the credit needle.
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
    print(f"🚀 Launching Unwatched Analyst (The Final Stand)...")
    filepath = "market_research.md"
    
    try:
        # PHASE 1: NICHE (Hardcoded to avoid Gemini 503 errors during this test)
        query_text = "Cursor AI coding agent tutorial 2026"
        
        # PHASE 2: SEARCH (1 Credit)
        # Endpoint: youtube/search | Parameter: text
        search_res = get_supadata("youtube/search", {"text": query_text})
        
        if not search_res or "videos" not in search_res or not search_res["videos"]:
            print("❌ Search failed or returned empty.")
            return

        # PHASE 3: SELECT
        videos = search_res["videos"]
        # Take the first one just to ensure we complete the loop
        top_video = videos[0]
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Targeted: {top_video['title']}")

        # PHASE 4: TRANSCRIPT (2 Credits)
        # Endpoint: youtube/transcript | Parameter: url
        t_res = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = t_res.get("content", "") if t_res else "No transcript."

        # PHASE 5: AI SYNTHESIS (Free)
        # We wrap this in a try-except because Gemini 503s are common right now
        try:
            report = client.models.generate_content(
                model=MODEL_ID, 
                contents=f"Trap/Fix for: {top_video['title']}\n{str(transcript_text)[:4000]}"
            ).text
        except:
            report = "AI Synthesis failed (503), but data was captured."

        # PHASE 6: THE SAVE (Force UTF-8)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{report}")
        
        print(f"✅ SUCCESS! Check {filepath}. Usage should be ~14.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
