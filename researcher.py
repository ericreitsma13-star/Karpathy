import os
import time
import requests
from google import genai
from datetime import datetime

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.5-flash-lite'

def get_supadata(endpoint, params):
    url = f"https://api.supadata.ai/v1/{endpoint}"
    headers = {"x-api-key": SUPA_KEY}
    time.sleep(2.5)
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"📡 API Call: {endpoint} | Status: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        print(f"⚠️ Error: {response.text}")
        return None
    except Exception as e:
        print(f"❌ Network Error: {e}")
        return None

def scout_market():
    print("🚀 Launching Unwatched Analyst...")
    filepath = "market_research.md"

    query_text = "Cursor AI coding agent tutorial 2026"

    # PHASE 2: SEARCH — correct param is 'query', not 'text'
    search_res = get_supadata("youtube/search", {"query": query_text})

    if not search_res or "videos" not in search_res or not search_res["videos"]:
        print("❌ Search failed or returned empty.")
        print(f"   Raw response: {search_res}")
        return

    top_video = search_res["videos"][0]
    video_url = f"https://www.youtube.com/watch?v={top_video['id']}"
    print(f"📊 Targeted: {top_video['title']}")

    # PHASE 4: TRANSCRIPT — text=true returns plain string instead of chunked array
    t_res = get_supadata("youtube/transcript", {"url": video_url, "text": "true"})

    if t_res:
        # When text=true, content is a plain string
        transcript_text = t_res.get("content", "No transcript.")
    else:
        transcript_text = "No transcript."

    # PHASE 5: AI SYNTHESIS
    try:
        report = client.models.generate_content(
            model=MODEL_ID,
            contents=f"Summarise key insights from this YouTube video titled '{top_video['title']}':\n{str(transcript_text)[:4000]}"
        ).text
    except Exception as e:
        report = f"AI Synthesis failed: {e}"

    # PHASE 6: SAVE
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {top_video['title']}\n")
        f.write(f"**URL:** {video_url}\n\n{report}")

    print(f"✅ Done. Check {filepath}.")

if __name__ == "__main__":
    scout_market()
