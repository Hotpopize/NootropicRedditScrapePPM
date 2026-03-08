# NootropicRedditScrapePPM: Qualitative Analysis System

This repository contains a specialized software tool designed for the qualitative analysis of Reddit discussions, focusing specifically on the Push-Pull-Mooring (PPM) framework within the context of cognitive supplement usage. It facilitates academic research by providing tools for systematic data collection, standardized qualitative coding, and thematic visualization.

## 🌟 Research Justification

Traditional qualitative research methodologies often face significant scalability limits when analyzing digital spaces like Reddit. This application addresses these constraints by combining programmatic data collection with automated qualitative thematic coding via locally hosted language models.

**Key Academic Benefits:**

- **Data Privacy & Security:** All computation, including language model inference, is executed locally (via Ollama). No research data is transmitted to external servers, adhering strictly to institutional review board (IRB) privacy guidelines.
- **Methodological Transparency:** The system is designed to provide complete auditability. Researchers can trace coding decisions back to specific, defined frameworks within the codebook, facilitating robust methodology sections.
- **Scalability of Analysis:** Automates the initial categorization of large datasets (thousands of posts) according to the PPM framework, allowing researchers to focus on higher-level thematic analysis.

## 📂 System Architecture

The repository is structured to separate concerns into independent components:

- **`app.py`**: The primary application interface. Initializes the interactive dashboard for research configuration.
- **`modules/`**: Data collection and processing scripts (e.g., API integration, automated qualitative coding pipelines, and visualization rendering).
- **`core/`**: Underlying data models (Pydantic schemas) and database connection handlers.
- **`data/`**: Local repository for SQLite databases (`research_data.db`), ensuring data privacy and localized storage.
- **`docs/`**: Project documentation, methodology definitions, and instructional guides.

## 🚀 Installation & Initialization

You will need **Python** (version 3.11 or higher) and **Ollama** installed on your local environment to run the analysis engine.

### Setup Instructions

1. **Clone the Repository:**

   ```bash
   git clone <repository-url>
   cd NootropicRedditScrapePPM
   ```

2. **Initialize Virtual Environment:**

   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # macOS/Linux
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Launch the Interface:**

   ```bash
   streamlit run app.py
   ```

For detailed protocols regarding data collection paradigms, model selection, and dataset exportation, please consult the [**User Guide**](docs/user_guide.md).

## 📄 Licensing & Academic Usage

See [LICENSE](LICENSE) file for distribution details. Please reference the methodology documentation if utilizing this tool in published work.
