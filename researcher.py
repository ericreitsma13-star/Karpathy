import os
import requests
from google import genai
from datetime import datetime

# 1. API Configuration
# These are pulled from your GitHub Secrets
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

# 2. Client Setup (2026 Unified SDK)
client = genai.Client(api_key=GEMINI_KEY)

# Using your verified 2.5 Flash Lite quota for the scout (Unlimited RPD)
# Using 2.5 Flash for the deep analyst (10k RPD)
SCOUT_MODEL = 'gemini-2.5-flash-lite'
ANALYST_MODEL = 'gemini-2.5-flash'

def get_supadata(endpoint, params):
    """Fetcher for Supadata.ai with error handling"""
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Supadata {endpoint} error: {response.status_code}")
        return None
    except Exception as e:
        print(f"❌ Supadata Request Failed: {e}")
        return None

def scout_market():
    print(f"🚀 Launching Unwatched Analyst...")
    
    try:
        # PHASE 1: Niche Generation
        niche_prompt = "Identify a 2026 tech niche with high knowledge debt. Return ONLY a YouTube search query."
        res = client.models.generate_content(model=SCOUT_MODEL, contents=niche_prompt)
        query = res.text.strip().replace('"', '')
        print(f"🔍 Searching: {query}")

        # PHASE 2: The Corrected 2026 Search Call
        # In 2026, Supadata uses the /youtube/search endpoint or 
        # the /metadata endpoint with a 'search' parameter.
        search_res = get_supadata("youtube/search", {"query": query})
        
        # If still 404, Supadata's latest API uses 'metadata' as the search hub
        if not search_res:
            print("🔄 404 hit, trying 'metadata' search fallback...")
            search_res = get_supadata("metadata", {"search": query})

        if not search_res or "videos" not in search_res:
            print(f"❌ Could not find videos for '{query}'. Trying a broader term...")
            search_res = get_supadata("metadata", {"search": "Software Engineering Traps 2026"})

        videos = search_res.get("videos", []) if search_res else []
        
        if not videos:
            print("❌ Absolute failure to find videos. Check Supadata Dashboard for API changes.")
            return
            
        # Sort by comment count to find the frustrated users
        top_video = max(videos, key=lambda x: int(x.get('commentCount', 0)) if x.get('commentCount') else 0)
        video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
        comment_count = top_video.get('commentCount', 0)
        
        print(f"📊 Targeted: {top_video['title']}")
        print(f"💬 Signal: {comment_count} comments found.")

        # PHASE 3: Deep Intel Extraction
        # Pull clean transcript for RAG analysis
        transcript_data = get_supadata("transcript", {"url": video_url, "text": "true"})
        transcript_text = transcript_data.get("content", "") if transcript_data else "No transcript available."

        # PHASE 4: Synthesis (The Analyst Brain)
        analysis_prompt = (
            f"Analyze this content for the 'Unwatched' App.\n"
            f"Video Title: {top_video['title']}\n"
            f"Frustrated User Count: {comment_count}\n"
            f"Transcript Snippet: {str(transcript_text)[:8000]}\n\n"
            "Identify:\n"
            "1. THE TRAP: Why is this video misleading or outdated in 2026?\n"
            "2. THE FIX: What is the specific 'Skill Block' Unwatched should provide?\n"
            "3. SIGNAL SCORE: 1-10 based on user frustration.\n"
            "Output in Markdown format."
        )
        
        report = client.models.generate_content(model=ANALYST_MODEL, contents=analysis_prompt)

        # PHASE 5: Update the Market Intelligence Map
        with open("market_research.md", "a") as f:
            f.write(f"\n\n---\n### 📈 Trend: {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
            f.write(f"**Video URL:** {video_url}\n")
            f.write(f"**Social Signal:** {comment_count} comments\n\n")
            f.write(report.text)
        
        print(f"✅ Market Intelligence Map Updated. Found a {top_video['title']} lead.")

    except Exception as e:
        print(f"❌ Execution Error: {e}")

if __name__ == "__main__":
    if not GEMINI_KEY or not SUPA_KEY:
        print("❌ CRITICAL: Missing API keys in environment.")
    else:
        scout_market()
