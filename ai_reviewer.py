# ai_reviewer.py

import openai
import os
import sys
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def get_review_prompt(code: str, filename: str) -> str:
    file_type = "SQL" if filename.endswith(".sql") else "Python"
    return f"""
You are a senior software reviewer. Analyze this {file_type} code file.

Identify and comment on:
- Poor variable/function naming with better suggestions
- Syntax or logical errors
- Redundant or unreachable code
- Performance or optimization opportunities
- Missing edge-case or error handling
- Security and style violations

Provide clear, bullet-point feedback with explanations.

Filename: {filename}

Code:
{code}
"""

def analyze_file(file_path: str):
    try:
        with open(file_path, "r") as f:
            code = f.read()
    except Exception as e:
        return f"❌ Error reading file `{file_path}`: {e}"

    prompt = get_review_prompt(code, file_path)

    try:
        print(f"\n🔍 Reviewing: {file_path}")
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a professional code reviewer."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )
        print(response.choices[0].message.content.strip())
    except Exception as e:
        print(f"❌ OpenAI error for {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ai_reviewer.py <file1.sql> [file2.py] ...")
    else:
        for file in sys.argv[1:]:
            analyze_file(file)
