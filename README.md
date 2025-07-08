#### AI-Powered Code Reviewer

This project tackles a real-world inefficiency in the development workflow: manual code reviews of Snowflake SQL, Python stored procedures, and Snowpark logic.

Using OpenAI’s GPT-4 API, this solution automates the code review process—enhancing productivity, reducing repetitive work, and providing scalable AI feedback across roles.


## 💡Use Case: AI-Powered Code Review Assistant
🔧 Problems Faced (Manual Workflow)

As a team lead, I spend significant time reviewing code changes. Often:

- Reviewing repetitive code patterns (e.g., null checks, naming conventions, repeated logic)
- Commenting on similar issues repeatedly across PRs
- Lack consistency in quality
- Miss hidden inefficiencies (like SELECT * or full table scans)
- Missing subtle anti-patterns or performance concerns due to fatigue or time pressure
- Consume bandwidth of experienced developers
- Context-switching across multiple PRs, teams, or modules

## ⏱ Inefficiency:

This process takes 2–3 hours/week per project and slows down PR turnaround time, affecting team velocity.


## 🎯 Hackathon Goal

Automate code review for Code reviews using AI to:

- Identify anti-patterns in SQL and Snowpark logic
- Reduce time spent on manual reviews
- Provide scalable, explainable feedback for teams


## 🤖 AI-Based Solution Overview

## 🛠️ Tooling:
CodiumAI / DeepCode / CodeRabbit – LLM-based GitHub-integrated code review bots
Or DIY with OpenAI GPT-4, repo parser (e.g., Tree-sitter), and GitHub PR API

## 🧠 How It Works (High-Level):
AI model reads a Pull Request diff
Identifies issues like poor naming, missing edge-case handling, redundant logic, security or style violations
Auto-generates comments or suggestions directly on the PR or outputs a markdown summary
Can be fine-tuned to learn team-specific patterns or integrate into CI pipelines



## ✅ Key Features

- GPT-4 powered reviews of .sql and .py files

- Highlights common issues:
  -  SELECT * usage
  -  Missing WHERE clauses
  -  Inefficient joins or CTEs
  -  Python error-handling gaps
  -  Snowpark anti-patterns

Two interfaces:
  -  CLI for developers
  -  Streamlit app for user-friendly feedback

-  GitHub Action integration for PR automation


## 🛠️ Tech Stack

-  Python 3.10+
-  OpenAI API
-  Streamlit
-  GitHub Actions
-  dotenv for environment config


## 🚀 How to Run (Local Demo)

1. Clone and set up environment
```
git clone https://github.com/your-org/snowflake-ai-reviewer.git
cd snowflake-ai-reviewer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Create .env file
```
OPENAI_API_KEY=sk-<your-key>
```

3. Run CLI script (multi-file support)
```
python ai_reviewer.py file1.sql file2.py
```

4. Run Streamlit app
```
streamlit run streamlit_app.py
```

Upload a file and click "🔍 Run AI Review" to view feedback.


