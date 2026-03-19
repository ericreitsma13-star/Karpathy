import os
import requests
from google import genai
from datetime import datetime

# 1. Setup - Explicitly using the current stable 2026 ID
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

# The most stable ID for free-tier automation in 2026
MODEL_ID = 'gemini-2.0-flash-lite' 

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Supadata {endpoint} failed: {response.status_code}")
        return None
    except:
        return None

def scout_market():
    print(f"🚀 Starting Unwatched Scout via {MODEL_ID}...")
    
    try:
        # PHASE 1: Niche Generation
        prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
        res = client.models.generate_content(model=MODEL_ID, contents=prompt)
        query = res.text.strip().replace('"', '')
        print(f"🔍 Searching: {query}")

        # PHASE 2: Search (Supadata Search)
        # Trying 'youtube/search' then 'search' as fallback
        search_results = get_supadata("youtube/search", {"query": query})
        if not search_results or "videos" not in search_results:
            search_results = get_supadata("search", {"query": query, "type": "video"})

        videos = search_results.get("videos", []) if search_results else []
        if not videos:
            print("❌ No videos found. Check Supadata search credits.")
            return

        # PHASE 3: Intel Gathering
        # Target the video with the most comments (Social Proof)
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']}")

        transcript_data = get_supadata("transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_data.get("content", "") if transcript_data else "No transcript."

        # PHASE 4: Synthesis
        analysis_prompt = (
            f"Video: {top_video['title']}\n"
            f"Transcript: {str(transcript_text)[:5000]}\n\n"
            "Format as Markdown: Trap, Fix, Signal Score (1-10)."
        )
        report = client.models.generate_content(model=MODEL_ID, contents=analysis_prompt)

        # PHASE 5: Write to File
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{report.text}")
        
        print("✅ Market Intelligence Map Updated.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY.")
    else:
        scout_market()
