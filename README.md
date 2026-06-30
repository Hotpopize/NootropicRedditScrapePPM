# NootropicRedditScrapePPM
### Qualitative Research Instrument for Consumer Discourse Analysis
**Modul University Vienna — MSc Management / MBA Thesis Project**

---

**Principal Researcher:** Vladislav Dolgov (u/MscNooManMuAt)  
**Research Contact:** 1821019@modul.ac.at  
**Supervision:** Dr. Lyndon Nixon  
**Compliance Status:** Verified Alignment with the *Reddit Research Data Addendum* (Executed 2026-04-01).

---

## Project Overview

This instrument facilitates the systematic study of cognitive supplement (nootropic) discourse. It implements the **Push-Pull-Mooring (PPM)** migration framework to understand why consumers transition from traditional stimulants (e.g., caffeine, prescription meds) to natural nootropic alternatives.

The tool provides:

- **Methodological Consistency**: Automated thematic coding based on established qualitative frameworks.
- **Privacy-First Analysis**: Utilizes local Large Language Models (LLMs) to ensure research data never leaves the investigator's local environment.
- **Transparency**: A full audit trail of collection parameters, replicability logs, and data quality metrics.

## Pipeline Architecture

The research instrument is divided into sequential modules:

- **Module 1 (Ingestion)**: BigQuery BQAH extraction and CLI CSV import (see [bqah_extraction_reference.md](file:///c:/Users/iamab/Documents/MasterNOOoffline/NootropicRedditScrapePPM/NootropicRedditScrapePPM/docs/bqah_extraction_reference.md) for technical parameters).
- **Module 2 (Coding)**: Automated deductive and emergent thematic coding via local LLMs.
- **Module 3/4 (Aggregation & Audit)**: Quantitative matrix generation (Chapter 4 tables) and compliance auditing.
- **Module 5 (The Ultrathinking Council)**: A multi-agent LLM orchestrator to synthesize the Chapter 4 tables into deep qualitative insights through persona-driven debate.

## Ethics & Compliance

This project is built on the principle of **Public Code, Private Data**. It adheres strictly to the Reddit Data API Terms of Service. For a full audit of how every clause of the *Reddit Research Data Addendum* is operationalised, see the **Compliance Map (COMPLIANCE.md)**.

- **Non-Commercial Use**: This software is an academic artifact and is governed by the [MIT License](LICENSE).
- **PII Protection**: Raw usernames are never stored in the analysis pipeline. All `author` fields are irreversibly pseudonymized using SHA256 hashing at the point of ingestion.
- **Data Deletion Workflow**: Includes database deletion options to ensure the dataset remains compliant with right-to-be-forgotten requirements.
- **No Redistribution**: This repository does **not** contain or redistribute real Reddit user data.

## Quickstart (Reviewer Demo)

Because this tool requires private research datasets, we provide a CLI script to generate a **100% synthetic dataset** to allow immediate evaluation of the analysis pipeline.

```bash
# 1. Install dependencies (requires Python 3.11+)
pip install -e .

# 2. Verify installation and connection health
python scripts/verify_install.py

# 3. Ingest the synthetic demo data into the database
python scripts/generate_mock_ppm_data.py

# 4. Launch the research dashboard
streamlit run app.py
```

Once imported, the dashboard will display the synthetic session, where you can test the **LLM Coder** and **PPM Visualization** modules.

## Configuration

For full research deployment (local LLM qualitative coding), please refer to the SETUP.md guide for instructions on:

- Configuring local LLMs via Ollama.
- Managing environment variables.

---

## Citation

If you use this instrument or methodology in your research, please cite as follows:

```bibtex
@software{dolgov2026caffeine,
  author = {Dolgov, Vladislav},
  title = {Caffeine to Brain Boosts: A Qualitative Research Instrument for Nootropic Discourse Analysis},
  institution = {Modul University Vienna},
  year = {2026},
  url = {https://github.com/Hotpopize/NootropicRedditScrapePPM}
}
```
