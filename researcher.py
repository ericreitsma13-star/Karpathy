import os
import requests
from google import genai
from datetime import datetime

# 1. Setup
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

# Verified 2026 Model IDs
MODEL_ID = 'gemini-2.5-flash-lite' 

def get_supadata(endpoint, params):
    """Fetcher for Supadata.ai with 2026 parameter mapping"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    
    # Debug: See what we are sending
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        print(f"⚠️ Supadata {endpoint} Error {response.status_code}: {response.text}")
        return None
    return response.json()

def scout_market():
    print(f"🚀 Launching Unwatched Analyst...")
    
    try:
        # PHASE 1: Niche Discovery
        prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
        res = client.models.generate_content(model=MODEL_ID, contents=prompt)
        query = res.text.strip().replace('"', '')
        print(f"🔍 Searching: {query}")

        # PHASE 2: Verified 2026 Search Call
        # Endpoint: /youtube/search | Parameter: text (Required)
        search_res = get_supadata("youtube/search", {"text": query})
        
        # If the niche is too narrow, fallback to a broader high-signal search
        if not search_res or not search_res.get("videos"):
            print("🔄 Niche too narrow, trying 'Software Traps 2026'...")
            search_res = get_supadata("youtube/search", {"text": "Software Engineering Traps 2026"})

        videos = search_res.get("videos", []) if search_res else []
        if not videos:
            print("❌ Absolute failure to find videos. Check API Key/Credits.")
            return

        # PHASE 3: Target High-Pain (Most Comments)
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        print(f"📊 Analyzing: {top_video['title']} ({top_video.get('commentCount')} comments)")

        # PHASE 4: Transcript & Analysis
        # Endpoint: /transcript | Parameter: url (Required)
        transcript_data = get_supadata("transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_data.get("content", "") if transcript_data else "No transcript."

        analysis_prompt = (
            f"Analyze for Unwatched App:\nVideo: {top_video['title']}\n"
            f"Transcript: {str(transcript_text)[:8000]}\n\n"
            "Output Markdown: Trap, Fix, Signal Score (1-10)."
        )
        report = client.models.generate_content(model=MODEL_ID, contents=analysis_prompt)

        # PHASE 5: Save
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**URL:** {video_url}\n{report.text}")
        
        print("✅ Market Intelligence Map Updated.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    if not SUPA_KEY:
        print("❌ Missing SUPADATA_API_KEY.")
    else:
        scout_market()
