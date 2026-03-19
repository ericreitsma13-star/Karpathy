import os
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
    return response.json()

def scout_market():
    print("🚀 Starting Fully Autonomous Unwatched Scout...")
    model_id = 'gemini-2.5-flash'

    # PHASE 1: Identify the 'Trap' Niche
    niche_prompt = "Identify a 2026 tech niche with high knowledge debt (e.g. Claude MCP, PQC, Edge AI). Return ONLY a YouTube search query."
    query = client.models.generate_content(model=model_id, contents=niche_prompt).text.strip().replace('"', '')
    print(f"🔍 Searching YouTube for: {query}")

    # PHASE 2: Dynamic Search via Supadata
    # We use Supadata to find the top 3 videos for this query
    search_results = get_supadata("search", {"query": query, "type": "video"})
    videos = search_results.get("videos", [])

    if not videos:
        print("❌ No videos found for this niche.")
        return

    # PHASE 3: Process the most 'Painful' Video (Highest Comment Count)
    # Sort by comment count to find where the frustrated people are
    top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
    video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
    
    print(f"📊 Analyzing: {top_video['title']} ({top_video.get('commentCount')} comments)")

    # PHASE 4: Extract Intel
    transcript = get_supadata("transcript", {"url": video_url, "text": "true"})
    
    analysis_prompt = (
        f"Analyze this for the 'Unwatched' App:\n"
        f"Video: {top_video['title']}\n"
        f"Comments: {top_video.get('commentCount')}\n"
        f"Transcript: {str(transcript.get('content', ''))[:5000]}\n\n"
        "Output Markdown: 1. The Trap, 2. The Fix, 3. Target Audience (based on comments), 4. Signal Score."
    )
    
    report = client.models.generate_content(model=model_id, contents=analysis_prompt).text

    # PHASE 5: Log to Research Map
    with open("market_research.md", "a") as f:
        f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
        f.write(f"**URL:** {video_url}\n")
        f.write(report)
    
    print(f"✅ Research complete for: {top_video['title']}")

if __name__ == "__main__":
    scout_market()
