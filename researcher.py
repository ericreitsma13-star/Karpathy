import os
import time
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.5-flash-lite' 

def get_supadata(endpoint, query_val):
    """The 2026 Verified GET Fetcher"""
    # Endpoint is /youtube/search and parameter is strictly 'query'
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    params = {"query": query_val} # Requests will auto-encode this correctly
    
    time.sleep(2) # Cooldown for Free Tier
    
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
    print(f"🚀 Launching Unwatched Analyst (Clean Query Mode)...")
    
    try:
        # PHASE 1: Niche Discovery (Free)
        # We need a string that YouTube actually likes
        prompt = "Identify a 2026 tech niche with high tutorial debt. Return ONLY a 3-word search query string. No formatting."
        res = client.models.generate_content(model=MODEL_ID, contents=prompt)
        # Force clean the string: remove stars, quotes, and extra lines
        query_text = res.text.strip().split('\n')[0].replace('*', '').replace('"', '')
        print(f"🔍 Brainstormed: {query_text}")

        # PHASE 2: Discovery (Uses verified 'query' parameter)
        search_res = get_supadata("youtube/search", query_text)
        
        if not search_res or not search_res.get("videos"):
            print("🔄 Falling back to stable search term...")
            search_res = get_supadata("youtube/search", "Software Traps 2026")

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Absolute failure. Check credits at supadata.ai/dashboard")
            return

        # PHASE 3: Select Most Commented
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']}")

        # PHASE 4: Transcript (Uses 'url' parameter)
        # Note: We call get_supadata slightly differently here because it's a different param
        t_url = f"https://api.supadata.ai/v1/youtube/transcript"
        t_res = requests.get(t_url, headers={"x-api-key": SUPA_KEY}, params={"url": video_url, "text": "true"})
        transcript_text = t_res.json().get("content", "") if t_res.status_code == 200 else ""

        # PHASE 5: Synthesis & Save
        analysis = client.models.generate_content(
            model=MODEL_ID, 
            contents=f"Trap/Fix for: {top_video['title']}\n{str(transcript_text)[:5000]}"
        ).text

        with open("market_research.md", "a", encoding="utf-8") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n{analysis}")
        
        print("✅ SUCCESS! Check market_research.md")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
