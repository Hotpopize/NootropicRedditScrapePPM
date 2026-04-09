# Caffeine to Brain Boosts: Using Online Communities to Understand the Nootropics Market.

This repository contains a specialized software tool designed to support qualitative academic research by combining programmatic data collection with automated thematic text analysis. It facilitates the systematic collection of Reddit discussions and utilizes local Large Language Models (LLMs) to securely code qualitative data according to the Push-Pull-Mooring (PPM) framework.

By prioritizing academic rigor and data privacy, all computational analysis is executed locally without transmitting sensitive research data to external APIs. The application features a streamlined pipeline from data acquisition to codebook management, culminating in an interactive dashboard for rapid visualization of qualitative coding themes.

## Setup Instructions

Ensure you have Python 3.11+ and Ollama installed.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Hotpopize/NootropicRedditScrapePPM.git
   cd NootropicRedditScrapePPM
   ```

2. **Install dependencies:**
   ```bash
   pip install -e .
   ```

3. **Launch the application:**
   ```bash
   streamlit run app.py
   ```

## Configuration

Reddit API credentials are required. See SETUP.md for the Reddit4Researcher application process.

## Quickstart (Try it without Reddit credentials!)

Because of strict PII and platform requirements, attempting to use the scraper without API credentials will fail. 

However, you can test the **entire downstream qualitative analysis pipeline** using our provided `samples/synthetic_nootropics_sample.csv` (a 100% fabricated dataset simulating the Push-Pull-Mooring framework):

```bash
# 1. Start the Streamlit application
streamlit run app.py

# 2. Open a separate terminal and import the synthetic sample dataset
python scripts/import_external_data.py samples/synthetic_nootropics_sample.csv --acknowledge-pii-scrubbing
```

Now, open the dashboard. The dataset will exist under `External Import: synthetic_nootropics_sample.csv` where you can run the LLM Coder and Topic Modeler instantly!
