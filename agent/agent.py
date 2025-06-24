#!/usr/bin/env python3
import os, json, sys, datetime
import openai
from rich.console import Console, Group
from rich import print

console = Console()
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONTEXT_FILE = os.path.join(PROJECT_DIR, 'context.json')
LATEST_LOG = os.path.join(PROJECT_DIR, 'logs', 'latest.log')
HISTORY_LOG = os.path.join(PROJECT_DIR, 'logs', f'history-{datetime.datetime.now():%Y%m%d-%H%M%S}.log')

def load_context():
    if os.path.exists(CONTEXT_FILE):
        with open(CONTEXT_FILE) as f:
            return json.load(f)
    return {}

def ask_gpt(context, user_message):
    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    system_prompt = f"""You are a dev agent for the AOE2HD replay parser project.

Context:
{json.dumps(context, indent=2)}

Answer with concise, technical insights only."""
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            max_tokens=800
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"[ERROR] OpenAI call failed: {str(e)}"

def log_output(msg):
    with open(LATEST_LOG, 'w') as f:
        f.write(msg)
    with open(HISTORY_LOG, 'w') as f:
        f.write(msg)

def main():
    console.rule("[bold cyan]🧠 AOE2HD Parser Agent Started")
    context = load_context()
    user_message = sys.argv[1] if len(sys.argv) > 1 else "Summarize the replay parsing pipeline."
    answer = ask_gpt(context, user_message)
    console.print(f"\n[bold green]💬 GPT Response:[/bold green]\n{answer}\n")
    log_output(answer)

if __name__ == "__main__":
    main()
