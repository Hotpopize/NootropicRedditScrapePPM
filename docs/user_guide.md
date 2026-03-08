# NootropicRedditScrapePPM: User Guide for Researchers

Welcome to the NootropicRedditScrapePPM application. This guide will help you, as a non-technical researcher, navigate the tool to collect, analyze, and export Reddit data using the Push-Pull-Mooring (PPM) framework.

## 1. Getting Started

### Prerequisites

Before using the application, ensure that you have the following installed:

1. **Python 3.11** or higher.
2. **Git** (to download the repository).
3. **Ollama** (running on your local machine). Ollama provides the local inference engine required for the automated semantic coding features of this framework.

### Installation & Running from GitHub

If you are accessing this project from GitHub, follow these steps to download and run the application:

1. **Download the Code:**
    Open your terminal (or Command Prompt) and clone the repository:

    ```bash
    git clone https://github.com/your-username/NootropicRedditScrapePPM.git
    cd NootropicRedditScrapePPM
    ```

2. **Set Up the Environment:**
    We recommend using a virtual environment to manage dependencies:

    ```bash
    # Create the virtual environment
    python -m venv .venv

    # Activate it (Windows)
    .venv\Scripts\activate
    # OR (macOS/Linux)
    source .venv/bin/activate
    ```

3. **Install Dependencies:**
    Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    # OR if requirements.txt is missing, use:
    pip install .
    ```

4. **Run the Application:**
    Once everything is installed and Ollama is running in the background, start the interface:

    ```bash
    streamlit run app.py
    ```

    Once running, a web browser window will open automatically, displaying the application interface.

## 2. Navigating the Application

The application is divided into five main modules, accessible from the **Navigation** sidebar on the left:

### 📊 Dashboard

The Dashboard provides a high-level overview of your collected and coded data corpus. Following data acquisition and automated analysis, this page will display visualizations such as market segmentation trends, common discourse keywords, and the quantitative breakdown of Push, Pull, and Mooring factors.

### 🌐 Reddit Data Collection

This module is where you gather raw data from Reddit.

- **Search Terms:** Enter the specific keywords or topics you are researching (e.g., "Nootropics", "Lion's Mane").
- **Subreddits:** Specify which Reddit communities to search within (e.g., "Nootropics").
- **Run Collection:** Click the button to start fetching posts and comments. The tool will save this raw data to the local database securely.

### 🤖 Automated Qualitative Coding

This module serves as the primary computational engine. Here, the selected language model systematically evaluates the gathered dataset against your established Codebook parameters.

- **Select Model:** Choose the computational language model you wish to deploy.
- **Start Coding:** The system will process the raw text corpus and programmatically apply tags for appropriate PPM factors and emergent themes based on the Codebook definitions.
- *Note: Computational duration scales with system hardware resources and the volume of the text corpus.*

### 📖 Codebook Management

The Codebook defines the foundational ontology the computational model utilizes to classify discourse. Ontological consistency is paramount to rigorous qualitative research.

- **View Codebook:** Review the current operational definitions for Push, Pull, and Mooring factors.
- **Edit/Refine:** If the automated coding lacks localized nuance or if emergent themes arise from the data distribution, you must update the definitions here to calibrate future coding accuracy.

### 💾 Data Export & Audit

Once your analysis is complete, you can export the data for use in other software (like Excel, SPSS, or NVivo) or for your academic manuscript.

- **Format options:** Choose your preferred format (CSV or JSON).
- **Zotero Integration:** If configured by your team, you can export relevant findings or links directly to your Zotero reference manager.

## 3. Best Practices & Tips

- **Iterative Coding:** Originate your process by gathering and coding a small, representative sample of data. Review the automated outputs. If the model mischaracterizes variables, return to the Codebook Management module and refine specific rules or provide demonstrative examples. Subsequently, clear the preliminary codes and execute across your full operational dataset.
- **Audit Trails:** Always export your final dataset and codebook versions. This provides a transparent, replicable audit trail appropriate for the methodology section of your manuscript.
- **System Check:** The sidebar will show a system status indicating if Ollama is running and models are installed. Ensure this is green before starting coding tasks.
