import os
from openai import OpenAI
from duckduckgo_search import DDGS

token = os.environ.get("GITHUB_TOKEN")
client = OpenAI(base_url="https://models.inference.ai.azure.com", api_key=token)

SUBJECTS = [
    "Edge AI Deployment (Small Models)",
    "Post-Quantum Cryptography Migration",
    "GreenOps & Carbon-Aware Coding",
    "Claude Code & MCP Infrastructure"
]

def run_scored_research():
    final_report = f"## 📈 Trend Analysis: {os.popen('date').read()}\n"
    leaderboard = []

    for subject in SUBJECTS:
        print(f"🧐 Scoring Subject: {subject}...")
        
        # Phase 1: Search for real human pain
        with DDGS() as ddgs:
            results = [r for r in ddgs.text(f"{subject} error 2026 site:reddit.com", max_results=3)]
        
        snippets = "\n".join([f"- {r['title']}: {r['body']}" for r in results])

        # Phase 2: GPT-5 calculates the 'Pain Score'
        scoring_prompt = f"""
        Analyze these search results for {subject}:
        {snippets}
        
        Task: 
        1. Rate the 'Knowledge Debt' from 1-10 (How broken are current tutorials?).
        2. Identify the 'Toxic Advice' (What old method is causing the error?).
        3. Output only: SCORE | TOXIC_ADVICE | SUMMARY
        """
        
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": scoring_prompt}]
        )
        
        score_data = res.choices[0].message.content
        leaderboard.append((subject, score_data))

    # Phase 3: Sort by Score and Save
    leaderboard.sort(key=lambda x: x[1].split('|')[0], reverse=True)
    
    with open("market_research.md", "a") as f:
        f.write(final_report)
        for sub, data in leaderboard:
            f.write(f"### [{sub}]\n{data}\n\n")

if __name__ == "__main__":
    run_scored_research()
