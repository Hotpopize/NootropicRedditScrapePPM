# Technical Setup Guide

This document provides detailed instructions for configuring the environment required by the **Caffeine to Brain Boosts** research instrument.

## Prerequisites

- **Python**: version 3.11 or higher.
- **Git**: for cloning the repository.
- **Ollama**: (Optional but recommended) for local qualitative coding. [Download Ollama here](https://ollama.com/).

## 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/Hotpopize/NootropicRedditScrapePPM.git
cd NootropicRedditScrapePPM

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
```

## 2. Reddit API Credentials (PRAW)

This tool requires authenticated access via the Reddit Data API. 

1. Visit [Reddit App Preferences](https://www.reddit.com/prefs/apps).
2. Create a new app of type **"script"**.
3. Note your `client_id` (under the script name) and `client_secret`.
4. Create a `.env` file in the project root:

```env
REDDIT_CLIENT_ID=your_id_here
REDDIT_CLIENT_SECRET=your_secret_here
REDDIT_USER_AGENT=Hotpopize_Thesis_Research_v1.0
```

> [!IMPORTANT]
> This tool strictly adheres to the **Reddit Research Data Addendum**. Ensure your `REDDIT_USER_AGENT` includes your researcher/project name and version.

## 3. Local LLM Configuration (Ollama)

By default, the automated qualitative coding module uses **Llama3** via Ollama.

1. Ensure Ollama is running on your machine.
2. Pull the required model:
   ```bash
   ollama pull llama3
   ```
3. If using a custom model, update the classification settings in the sidebar of the dashboard.

## 4. Running the Tool

```bash
streamlit run app.py
```
