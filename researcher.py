import os
import json
import subprocess
import time
import requests
from google import genai
from datetime import datetime
from pathlib import Path

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
SUPA_KEY = os.environ.get("SUPADATA_API_KEY")

SYNTHESIS_MODEL = 'gemini-2.5-flash'
HEADERS = {"x-api-key": SUPA_KEY}
MAX_REPAIR_ITERATIONS = 3
SKILLS_DIR = Path("skills")
NEXT_QUERY_FILE = Path("next_query.txt")

# Seed queries — only used if next_query.txt doesn't exist yet
SEED_QUERIES = [
    "apache iceberg python implementation 2026",
    "claude code MCP server custom tool build",
    "duckdb lakehouse pipeline python 2026",
    "pyspark delta lake streaming real implementation",
    "fastapi data engineering REST lakehouse",
]

GOAL = (
    "Building reusable Python skills for: data engineering (PySpark, Delta Lake, "
    "Apache Iceberg, DuckDB, FastAPI), and agentic LLM tooling (Claude Code, MCP servers, "
    "RAG pipelines). High-demand topics with thin or outdated coverage are preferred."
)

def supadata_get(endpoint, params):
    time.sleep(2.5)
    r = requests.get(f"https://api.supadata.ai/v1/{endpoint}", headers=HEADERS, params=params)
    print(f"📡 {endpoint} | {r.status_code}")
    if r.status_code == 200:
        return r.json()
    print(f"⚠️ {r.text}")
    return None

def run_tests():
    result = subprocess.run(
        ["python", "-m", "pytest", "test_proposed_skill.py", "-v", "--tb=short"],
        capture_output=True, text=True
    )
    return result.returncode == 0, result.stdout + result.stderr

def extract_skill_json(text):
    clean = text.strip().replace("```json", "").replace("```python", "").replace("```", "")
    return json.loads(clean)

def write_skill_files(skill_data):
    with open("proposed_skill.py", "w") as f:
        f.write(skill_data["implementation_code"])
    with open("test_proposed_skill.py", "w") as f:
        f.write(f"import sys\nfrom proposed_skill import *\n\n{skill_data['unit_test']}")

def get_existing_skills():
    SKILLS_DIR.mkdir(exist_ok=True)
    skills = [p.stem for p in SKILLS_DIR.glob("*.py") if not p.stem.startswith("test_")]
    return skills

def save_skill_to_registry(skill_data, video):
    SKILLS_DIR.mkdir(exist_ok=True)
    slug = skill_data["skill_name"].lower().replace(" ", "_").replace("-", "_")
    skill_path = SKILLS_DIR / f"{slug}.py"
    test_path = SKILLS_DIR / f"test_{slug}.py"

    with open(skill_path, "w") as f:
        f.write(f'"""\nSkill: {skill_data["skill_name"]}\n')
        f.write(f'Source: https://youtube.com/watch?v={video["id"]}\n')
        f.write(f'Title: {video["title"]}\n')
        f.write(f'Added: {datetime.now().strftime("%Y-%m-%d")}\n"""\n\n')
        f.write(skill_data["implementation_code"])

    with open(test_path, "w") as f:
        f.write(f"import sys\nfrom skills.{slug} import *\n\n{skill_data['unit_test']}")

    print(f"📚 Saved to registry: skills/{slug}.py")

def is_relevant(skill_name, existing_skills):
    existing_str = ", ".join(existing_skills) if existing_skills else "none yet"
    prompt = (
        f"Goal: {GOAL}\n\n"
        f"Existing skills already in registry: {existing_str}\n\n"
        f"Proposed new skill: '{skill_name}'\n\n"
        "Is this skill genuinely new (not a duplicate) and relevant to the goal? "
        "Reply with only YES or NO."
    )
    response = client.models.generate_content(model=SYNTHESIS_MODEL, contents=prompt)
    answer = response.text.strip().upper()
    print(f"🎯 Relevance check for '{skill_name}': {answer}")
    return answer.startswith("YES")

def generate_next_query(skill_name, existing_skills):
    existing_str = ", ".join(existing_skills) if existing_skills else "none yet"
    prompt = (
        f"Goal: {GOAL}\n\n"
        f"Skills already learned: {existing_str}\n"
        f"Most recently learned: '{skill_name}'\n\n"
        "What is the single most valuable next YouTube search query to run tomorrow, "
        "to find a high-demand topic with thin coverage that builds toward the goal? "
        "Reply with only the search query string, nothing else."
    )
    response = client.models.generate_content(model=SYNTHESIS_MODEL, contents=prompt)
    next_query = response.text.strip().strip('"')
    print(f"🔮 Next query: {next_query}")
    return next_query

def log_result(skill_name, video, iterations, test_output, relevant):
    with open("market_research.md", "a", encoding="utf-8") as f:
        f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {skill_name}\n")
        f.write(f"**Source:** https://youtube.com/watch?v={video['id']} — {video['title']}\n")
        f.write(f"**Relevant to goal:** {'✅ Yes' if relevant else '⏭ No — skipped'}\n")
        f.write(f"**Iterations to convergence:** {iterations}\n\n")
        f.write(f"```\n{test_output[:500]}\n```\n")

def scout_and_synthesize():
    print("🚀 [Karpathy Loop] Hunting for High-Signal Code...")

    # Use next_query.txt if it exists, else fall back to seed
    if NEXT_QUERY_FILE.exists():
        query = NEXT_QUERY_FILE.read_text().strip()
        print(f"📂 Using compounding query: {query}")
    else:
        query = SEED_QUERIES[0]
        print(f"🌱 Using seed query: {query}")

    existing_skills = get_existing_skills()
    print(f"📚 Registry has {len(existing_skills)} skill(s): {existing_skills or 'empty'}")

    search_res = supadata_get("youtube/search", {"query": query})
    if not search_res:
        return

    videos = [r for r in search_res.get("results", []) if r.get("type") == "video"]

    for video in videos:
        v_url = f"https://youtube.com/watch?v={video['id']}"

        t_res = supadata_get("youtube/transcript", {"url": v_url, "text": "true", "mode": "native"})
        if not t_res:
            continue

        transcript = t_res.get("content", "").lower()

        technical_markers = ["import", "def ", "async", "const", "npm", "pip", "install"]
        if not any(marker in transcript for marker in technical_markers):
            continue

        print(f"📊 High-Signal: {video['title']}")

        # FORWARD PASS
        prompt = (
            f"Goal: {GOAL}\n\n"
            f"Extract the exact technical implementation from this transcript:\n{transcript[:8000]}\n\n"
            "Ignore all marketing/intro. Focus on the code logic.\n"
            "Output ONLY a raw JSON object (no markdown fences) with:\n"
            "1. 'skill_name': short descriptive string\n"
            "2. 'implementation_code': complete, self-contained Python module. "
            "Only use stdlib or: requests, pytest. No other dependencies.\n"
            "3. 'unit_test': pytest test code. Do not import from proposed_skill — added automatically."
        )

        prediction = client.models.generate_content(model=SYNTHESIS_MODEL, contents=prompt)

        try:
            skill_data = extract_skill_json(prediction.text)
        except json.JSONDecodeError as e:
            print(f"❌ JSON parse failed: {e}\n--- RAW ---\n{prediction.text[:300]}\n---")
            continue

        # RELEVANCE CHECK — skip if duplicate or off-goal
        if not is_relevant(skill_data["skill_name"], existing_skills):
            log_result(skill_data["skill_name"], video, 0, "", relevant=False)
            continue

        write_skill_files(skill_data)
        print(f"⚙️  Skill '{skill_data['skill_name']}' staged. Running loss function...")

        # KARPATHY LOOP — backward pass until tests pass or max iterations
        for iteration in range(1, MAX_REPAIR_ITERATIONS + 1):
            passed, test_output = run_tests()

            if passed:
                print(f"✅ Tests passed on iteration {iteration}. Skill verified.")

                # Save to persistent registry
                save_skill_to_registry(skill_data, video)
                log_result(skill_data["skill_name"], video, iteration, test_output, relevant=True)

                # Generate next query — the loop compounds
                next_query = generate_next_query(skill_data["skill_name"], existing_skills)
                NEXT_QUERY_FILE.write_text(next_query)
                print(f"💾 Next query saved to {NEXT_QUERY_FILE}")
                return

            print(f"🔁 Iteration {iteration} failed. Feeding error back...")
            print(f"--- TEST OUTPUT ---\n{test_output[:400]}\n---")

            repair_prompt = (
                f"This Python skill failed its tests.\n\n"
                f"=== implementation_code ===\n{skill_data['implementation_code']}\n\n"
                f"=== unit_test ===\n{skill_data['unit_test']}\n\n"
                f"=== test output (loss) ===\n{test_output}\n\n"
                "Fix the implementation_code so all tests pass.\n"
                "Only use stdlib or: requests, pytest. No other dependencies.\n"
                "Output ONLY a raw JSON object with the same three keys: "
                "'skill_name', 'implementation_code', 'unit_test'."
            )

            repair = client.models.generate_content(model=SYNTHESIS_MODEL, contents=repair_prompt)

            try:
                skill_data = extract_skill_json(repair.text)
                write_skill_files(skill_data)
            except json.JSONDecodeError as e:
                print(f"❌ Repair JSON parse failed: {e}\n--- RAW ---\n{repair.text[:300]}\n---")
                break

        print(f"⚠️  Max iterations reached. Skill unstable — skipping to next video.")

    print("⚠️ No high-signal videos found in this batch. No skill staged.")

if __name__ == "__main__":
    scout_and_synthesize()
