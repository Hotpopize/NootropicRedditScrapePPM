# Technical Setup Guide

This document provides instructions for configuring the environment required by the qualitative research instrument.

## Prerequisites

- **Python**: version 3.11 or higher.
- **Git**: for version control.
- **Ollama**: for local qualitative coding.

## 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/Hotpopize/NootropicRedditScrapePPM.git
cd NootropicRedditScrapePPM

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
```

## 2. Local LLM Configuration (Ollama)

The automated qualitative coding module uses local Ollama models (e.g. Llama 3 or Gemma).

1. Ensure Ollama is running locally.
2. Pull the default model:
   ```bash
   ollama pull llama3
   ```
3. Custom models can be selected in the dashboard sidebar.

## 3. Running the Tool

To run the Streamlit dashboard:

```bash
streamlit run app.py
```
