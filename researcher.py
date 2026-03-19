import os
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

def get_supadata(endpoint, params):
    """Robust Supadata fetcher with error logging"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    response = requests.get(url, headers=headers, params=params)
    
    # Check if the server actually returned a success code
    if response.status_code != 200:
        print(f"❌ Supadata API Error {response.status_code}: {response.text}")
        return None

    try:
        return response.json()
    except Exception as e:
        print(f"❌ JSON Parse Error. Raw Response: {response.text[:200]}")
        return None

def scout_market():
    print("🚀 Starting Fully Autonomous Unwatched Scout...")
    model_id = 'gemini-2.0-flash-001' # Updated to latest stable

    # PHASE 1: Identify Niche
    try:
        niche_prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
        query_res = client.models.generate_content(model=model_id, contents=niche_prompt)
        query = query_res.text.strip().replace('"', '')
        print(f"🔍 Searching YouTube for: {query}")
    except Exception as e:
        print(f"⚠️ Gemini Thinking Error: {e}")
        return

    # PHASE 2: Dynamic Search
    # If this fails, it's likely the 'search' endpoint name changed to 'youtube/search' or similar
    search_results = get_supadata("youtube/search", {"query": query}) # Updated path
    
    if not search_results or "videos" not in search_results:
        # Fallback: Trying alternative endpoint if first one fails
        print("🔄 Trying alternative search endpoint...")
        search_results = get_supadata("search", {"query": query, "type": "video"})

    videos = search_results.get("videos", []) if search_results else []

    if not videos:
        print("❌ No videos found. Check Supadata API docs for 'search' endpoint path.")
        return

    # PHASE 3: Process Top Video
    top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
    video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
    print(f"📊 Analyzing: {top_video['title']} ({top_video.get('commentCount')} comments)")

    # PHASE 4: Transcript & Synthesis
    transcript_data = get_supadata("transcript", {"url": video_url, "text": "true"})
    transcript_text = transcript_data.get("content", "") if transcript_data else ""

    analysis_prompt = (
        f"Analyze for Unwatched App:\nVideo: {top_video['title']}\n"
        f"Transcript: {str(transcript_text)[:5000]}\n\n"
        "Output Markdown: Trap, Fix, Signal Score."
    )
    
    report = client.models.generate_content(model=model_id, contents=analysis_prompt).text

    # PHASE 5: Log
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
        f.write(f"**URL:** {video_url}\n{report}")
    
    print(f"✅ Research complete.")

if __name__ == "__main__":
    if not SUPA_KEY or not os.environ.get("GEMINI_API_KEY"):
        print("❌ Missing API Keys in Secrets.")
    else:
        scout_market()
