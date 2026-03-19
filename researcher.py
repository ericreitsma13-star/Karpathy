import os
import time
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.5-flash-lite' 

def safe_generate(prompt_text):
    """Retries Gemini calls if the server is 503 Busy"""
    for i in range(3): # Try 3 times
        try:
            return client.models.generate_content(model=MODEL_ID, contents=prompt_text)
        except Exception as e:
            if "503" in str(e):
                print(f"⏳ Server busy (503). Retrying in {10 * (i+1)}s...")
                time.sleep(10 * (i+1))
            else:
                raise e
    return None

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    time.sleep(2) # Supadata Cooldown
    response = requests.get(url, headers=headers, params=params)
    return response.json() if response.status_code == 200 else None

def scout_market():
    print(f"🚀 Launching Unwatched Analyst (Persistence Mode)...")
    
    try:
        # PHASE 1: Niche Discovery
        prompt = "Identify one 2026 tech niche with high knowledge debt. ONLY a 3-word search query."
        res = safe_generate(prompt)
        if not res: return
        
        query_text = res.text.strip().split('\n')[0].replace('*', '').replace('"', '')
        print(f"🔍 Brainstormed: {query_text}")

        # PHASE 2: Discovery
        search_res = get_supadata("youtube/search", {"query": query_text})
        videos = search_res.get("videos", []) if search_res else []
        
        if not videos:
            print("❌ No videos found. Check Supadata credits.")
            return

        # PHASE 3: Select & Analyze
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        print(f"📊 Analyzing: {top_video['title']}")

        # PHASE 4: Transcript
        transcript_res = get_supadata("youtube/transcript", {"url": f"https://youtube.com/watch?v={top_video['id']}", "text": "true"})
        transcript_text = transcript_res.get("content", "") if transcript_res else ""

        # PHASE 5: Synthesis
        analysis_res = safe_generate(f"Trap/Fix for: {top_video['title']}\n{str(transcript_text)[:5000]}")
        if not analysis_res: return

        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n{analysis_res.text}")
        
        print("✅ Success! Loop completed despite 503.")

    except Exception as e:
        print(f"❌ Script Error: {e}")

if __name__ == "__main__":
    scout_market()
