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
    """Verified 2026 GET fetcher"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    time.sleep(2) # Rate limit respect
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
    print(f"🚀 Launching Unwatched Analyst (Market-Fit Mode)...")
    
    try:
        # PHASE 1: Niche Discovery (Force a "Painful" Niche)
        # We steer away from 'Ethics' and toward 'Broken Code'
        prompt = (
            "Identify one 2026 developer niche where people are frustrated by broken tutorials "
            "(e.g., 'Cursor AI agent loops', 'Next.js 16 metadata errors', 'MCP server setup'). "
            "Return ONLY a 3-word YouTube search query."
        )
        res = client.models.generate_content(model=MODEL_ID, contents=prompt)
        query_text = res.text.strip().split('\n')[0].replace('*', '').replace('"', '')
        print(f"🔍 Searching for Pain: {query_text}")

        # PHASE 2: Discovery
        search_res = get_supadata("youtube/search", {"query": query_text})
        videos = search_res.get("videos", []) if search_res else []
        
        # Fallback to a guaranteed high-traffic technical term if niche fails
        if not videos:
            print("🔄 Niche too quiet. Falling back to 'LLM coding agent errors'...")
            search_res = get_supadata("youtube/search", {"query": "LLM coding agent errors"})
            videos = search_res.get("videos", []) if search_res else []

        if not videos:
            print("❌ Absolute failure. Check credits at supadata.ai/dashboard")
            return

        # PHASE 3: Select the "Most Hated" Video
        # Sorting by commentCount finds the 'Trap'
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']} ({top_video.get('commentCount')} comments)")

        # PHASE 4: Transcript
        transcript_res = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_res.get("content", "") if transcript_res else "No transcript."

        # PHASE 5: Synthesis
        analysis = client.models.generate_content(
            model=MODEL_ID, 
            contents=f"Identify the 'Trap' and 'Fix' for: {top_video['title']}\n{str(transcript_text)[:5000]}"
        ).text

        # PHASE 6: Write to Map
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{analysis}")
        
        print("✅ Success! Your Market Intelligence Map is updated.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
