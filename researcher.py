import os
import json
import subprocess
import time
import requests
from google import genai
from datetime import datetime

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")
MODEL_ID = 'gemini-2.0-flash'

HEADERS = {"x-api-key": SUPA_KEY}
MAX_REPAIR_ITERATIONS = 3

def supadata_get(endpoint, params):
    time.sleep(2.5)
    r = requests.get(f"https://api.supadata.ai/v1/{endpoint}", headers=HEADERS, params=params)
    print(f"📡 {endpoint} | {r.status_code}")
    if r.status_code == 200:
        return r.json()
    print(f"⚠️ {r.text}")
    return None

def run_tests():
    """Loss function: returns (passed, output)"""
    result = subprocess.run(
        ["python", "-m", "pytest", "test_proposed_skill.py", "-v", "--tb=short"],
        capture_output=True, text=True
    )
    passed = result.returncode == 0
    return passed, result.stdout + result.stderr

def extract_skill_json(text):
    clean = text.strip().replace("```json", "").replace("```python", "").replace("```", "")
    return json.loads(clean)

def write_skill_files(skill_data):
    with open("proposed_skill.py", "w") as f:
        f.write(skill_data["implementation_code"])
    with open("test_proposed_skill.py", "w") as f:
        f.write(f"import sys\nfrom proposed_skill import *\n\n{skill_data['unit_test']}")

def scout_and_synthesize():
    print("🚀 [Karpathy Loop] Hunting for High-Signal Code...")

    queries = [
        "python mcp server implementation 2026",
        "cursor ai composer complex app build",
        "fastapi document ocr backend live coding"
    ]

    # SEARCH — param is 'query', response key is 'results'
    search_res = supadata_get("youtube/search", {"query": queries[0]})
    if not search_res:
        return

    videos = [r for r in search_res.get("results", []) if r.get("type") == "video"]

    for video in videos:
        v_url = f"https://youtube.com/watch?v={video['id']}"

        t_res = supadata_get("youtube/transcript", {"url": v_url, "text": "true"})
        if not t_res:
            continue

        transcript = t_res.get("content", "").lower()

        technical_markers = ["import", "def ", "async", "const", "npm", "pip", "install"]
        if not any(marker in transcript for marker in technical_markers):
            continue

        print(f"📊 High-Signal: {video['title']}")

        # FORWARD PASS
        prompt = (
            f"Extract the exact technical implementation from this transcript:\n{transcript[:8000]}\n\n"
            "Ignore all marketing/intro. Focus on code logic.\n"
            "Output ONLY a raw JSON object (no markdown fences) with:\n"
            "1. 'skill_name': string\n"
            "2. 'implementation_code': complete Python module as a string\n"
            "3. 'unit_test': pytest test code as a string (no imports for proposed_skill, those are added automatically)"
        )

        prediction = client.models.generate_content(model=MODEL_ID, contents=prompt)

        try:
            skill_data = extract_skill_json(prediction.text)
        except json.JSONDecodeError as e:
            print(f"❌ JSON parse failed: {e}")
            continue

        write_skill_files(skill_data)
        print(f"⚙️  Skill '{skill_data['skill_name']}' staged. Running loss function...")

        # KARPATHY LOOP: backward pass until tests pass or max iterations
        for iteration in range(1, MAX_REPAIR_ITERATIONS + 1):
            passed, test_output = run_tests()

            if passed:
                print(f"✅ Tests passed on iteration {iteration}. Skill verified.")
                log_result(skill_data["skill_name"], video, iteration, test_output)
                return

            print(f"🔁 Iteration {iteration} failed. Feeding error back (backward pass)...")

            repair_prompt = (
                f"This Python skill failed its tests.\n\n"
                f"=== implementation_code ===\n{skill_data['implementation_code']}\n\n"
                f"=== unit_test ===\n{skill_data['unit_test']}\n\n"
                f"=== test output (loss) ===\n{test_output}\n\n"
                "Fix the implementation_code so all tests pass.\n"
                "Output ONLY a raw JSON object with the same three keys: "
                "'skill_name', 'implementation_code', 'unit_test'."
            )

            repair = client.models.generate_content(model=MODEL_ID, contents=repair_prompt)

            try:
                skill_data = extract_skill_json(repair.text)
                write_skill_files(skill_data)
            except json.JSONDecodeError as e:
                print(f"❌ Repair JSON parse failed: {e}")
                break

        print(f"⚠️  Max iterations reached. Skill unstable — skipping.")
        return

def log_result(skill_name, video, iterations, test_output):
    with open("market_research.md", "a", encoding="utf-8") as f:
        f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {skill_name}\n")
        f.write(f"**Source:** https://youtube.com/watch?v={video['id']} — {video['title']}\n")
        f.write(f"**Iterations to convergence:** {iteration}\n\n")
        f.write(f"```\n{test_output[:1000]}\n```\n")

if __name__ == "__main__":
    scout_and_synthesize()
