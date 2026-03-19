import os
from openai import OpenAI

# This grabs your github_pat_ from the Repo Secrets
token = os.environ.get("GITHUB_TOKEN")

client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=token
)

try:
    # We use gpt-5-mini: It's free on GitHub Models and 
    # smarter than GPT-4o for half the 'thought' cost.
    response = client.chat.completions.create(
        model="gpt-5-mini", 
        messages=[{"role": "user", "content": "Confirm: Unwatched is running on GPT-5."}]
    )
    print(f"AI Response: {response.choices[0].message.content}")
except Exception as e:
    print(f"Error: {e}")
