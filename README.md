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

**Reddit API credentials are entirely optional.** By default, the tool automatically utilizes a credential-free JSON endpoint mode to passively fetch data without requiring any configuration.

If you intend to perform rate-managed, high-volume data collection, you may configure Reddit Developer credentials using `.env.example` as a template.
