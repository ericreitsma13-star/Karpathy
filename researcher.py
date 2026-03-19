def scout_and_synthesize():
    print("🚀 [Karpathy Loop] Hunting for High-Signal Code...")

    queries = [
        "python mcp server implementation 2026",
        "cursor ai composer complex app build",
        "fastapi document ocr backend live coding"
    ]

    search_res = supadata_get("youtube/search", {"query": queries[0]})
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

    # ← correct position: after the for loop, only reached if no video passed the filter
    print("⚠️ No high-signal videos found in this batch. No skill staged.")


def log_result(skill_name, video, iterations, test_output):
    with open("market_research.md", "a", encoding="utf-8") as f:
        f.write(f"\n\n---\n### 📈 {datetime.now().strftime('%Y-%m-%d')} | {skill_name}\n")
        f.write(f"**Source:** https://youtube.com/watch?v={video['id']} — {video['title']}\n")
        f.write(f"**Iterations to convergence:** {iterations}\n\n")
        f.write(f"```\n{test_output[:1000]}\n```\n")


if __name__ == "__main__":
    scout_and_synthesize()
