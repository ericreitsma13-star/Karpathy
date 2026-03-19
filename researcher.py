import os
import time
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except:
        return None

def safe_generate(prompt):
    """Retries on 429 errors"""
    # Using 1.5-flash because it has the most reliable 'Free' quota in 2026
    model_id = 'gemini-1.5-flash' 
    for attempt in range(3):
        try:
            return client.models.generate_content(model=model_id, contents=prompt)
        except Exception as e:
            if "429" in str(e):
                print(f"⏳ Quota hit, waiting 10s (Attempt {attempt+1}/3)...")
                time.sleep(10)
            else:
                raise e
    return None

def scout_market():
    print("🚀 Starting Quota-Proof Unwatched Scout...")
    
    # PHASE 1: Niche
    niche_prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
    res = safe_generate(niche_prompt)
    if not res: return
    
    query = res.text.strip().replace('"', '')
    print(f"🔍 Searching: {query}")

    # PHASE 2: Search (Trying 'youtube/search' first, then 'search')
    search_results = get_supadata("youtube/search", {"query": query})
    if not search_results or "videos" not in search_results:
        search_results = get_supadata("search", {"query": query, "type": "video"})

    videos = search_results.get("videos", []) if search_results else []
    if not videos:
        print("❌ No videos found via Supadata.")
        return

    # PHASE 3: Process Top Video
    top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
    video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
    print(f"📊 Analyzing: {top_video['title']}")

    # PHASE 4: Synthesis
    transcript_data = get_supadata("transcript", {"url": video_url, "text": "true"})
    transcript_text = transcript_data.get("content", "") if transcript_data else "No transcript available."

    analysis_prompt = (
        f"Analyze for Unwatched App:\nVideo: {top_video['title']}\n"
        f"Transcript: {str(transcript_text)[:5000]}\n\n"
        "Output Markdown: Trap, Fix, Signal Score."
    )
    
    report_res = safe_generate(analysis_prompt)
    if not report_res: return

    # PHASE 5: Log
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
        f.write(f"**URL:** {video_url}\n{report_res.text}")
    
    print("✅ Research complete.")

if __name__ == "__main__":
    scout_market()
