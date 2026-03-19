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
    """Fetcher using verified 2026 GET pattern"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    # 2026 Cooldown
    time.sleep(2) 
    
    # The Error 400 earlier said 'query: Required', so we use 'query'
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"⚠️ Supadata {endpoint} Error {response.status_code}: {response.text}")
        return None
    return response.json()

def scout_market():
    print(f"🚀 Launching Unwatched Analyst (Strict String Mode)...")
    
    try:
        # PHASE 1: Niche Discovery (Strict Prompting)
        # We tell Gemini to shut up and just give the string.
        prompt = (
            "Identify one 2026 tech niche with high knowledge debt. "
            "Respond with ONLY a 3-5 word YouTube search query. "
            "Do not include bullets, bolding, or explanations."
        )
        res = client.models.generate_content(model=MODEL_ID, contents=prompt)
        
        # Clean the output to ensure it's just a raw string
        query_text = res.text.strip().split('\n')[0].replace('*', '').replace('"', '')
        print(f"🔍 Brainstormed: {query_text}")

        # PHASE 2: Discovery
        # Endpoint: /youtube/search | Parameter: query
        search_res = get_supadata("youtube/search", {"query": query_text})
        
        if not search_res or not search_res.get("videos"):
            print("🔄 Falling back to generic search...")
            search_res = get_supadata("youtube/search", {"query": "2026 software engineering traps"})

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Absolute failure. Usage is likely still at 6.")
            return

        # PHASE 3: Select & Analyze
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']}")

        # PHASE 4: Transcript
        transcript_res = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_res.get("content", "") if transcript_res else "No transcript found."

        # PHASE 5: Synthesis & Save
        analysis = client.models.generate_content(
            model=MODEL_ID, 
            contents=f"Identify Trap/Fix for: {top_video['title']}\n{str(transcript_text)[:5000]}"
        ).text

        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n{analysis}")
        
        print("✅ Success! Usage should hit 7 or 8 now.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
