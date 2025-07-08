import streamlit as st
import openai
import os
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

st.set_page_config(page_title="AI Code Reviewer", page_icon="🤖")
st.title("🤖 AI-Powered Code Reviewer")
st.caption("Upload a `.sql` or `.py` file for GPT-4 analysis")

uploaded_file = st.file_uploader("Choose a code file", type=["sql", "py"])
if uploaded_file:
    code = uploaded_file.read().decode("utf-8")
    filename = uploaded_file.name
    file_type = "SQL" if filename.endswith(".sql") else "Python"

    with st.spinner("Analyzing code with GPT-4..."):
        prompt = f"""
You are a senior software reviewer. Analyze this {file_type} code:

Detect and comment on:
- Poor naming + suggestions
- Syntax or logic issues
- Redundant or unreachable code
- Optimization or performance issues
- Missing edge-case handling
- Security/style violations

Give clear bullet-point feedback with explanation.

Filename: {filename}

Code:
{code}
"""

        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a software review expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            feedback = response.choices[0].message.content.strip()
            st.markdown("### ✅ AI Feedback")
            st.markdown(f"```markdown\n{feedback}\n```")
        except Exception as e:
            st.error(f"OpenAI API error: {e}")
