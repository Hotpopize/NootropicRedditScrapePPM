# 🔬 NootropicRedditScrapePPM
### Qualitative Research Instrument for Consumer Discourse Analysis
**Modul University Vienna — MSc Management / MBA Thesis Project**

---

**Principal Researcher:** Vladislav Dolgov ([u/MscNooManMuAt](https://reddit.com/user/MscNooManMuAt))  
**Research Contact:** 1821019@modul.ac.at  
**Supervision:** Dr. Lyndon Nixon  
**Compliance Status:** **Verified Alignment** with the *Reddit Research Data Addendum* (Executed 2026-04-01).

---

## 🏛️ Project Overview

This instrument facilitates the systematic study of cognitive supplement (nootropic) discourse. It implements the **Push-Pull-Mooring (PPM)** migration framework to understand why consumers transition from traditional stimulants (e.g., caffeine, prescription meds) to natural nootropic alternatives.

The tool provides:

- **Methodological Consistency**: Automated thematic coding based on established qualitative frameworks.
- **Privacy-First Analysis**: Utilizes local Large Language Models (LLMs) to ensure research data never leaves the investigator's local environment.
- **Transparency**: A full audit trail of collection parameters, replicability logs, and data quality metrics.

## ⚖️ Ethics & Compliance

This project is built on the principle of **Public Code, Private Data**. It adheres strictly to the Reddit Data API Terms of Service. For a full audit of how every clause of the *Reddit Research Data Addendum* is operationalised, see the [**Compliance Map (COMPLIANCE.md)**](COMPLIANCE.md).

- **Non-Commercial Use**: This software is an academic artifact and is governed by the [PolyForm Noncommercial 1.0.0 License](LICENSE).
- **PII Protection**: Raw usernames are never stored in the analysis pipeline. All `author` fields are irreversibly pseudonymized using SHA256 hashing at the point of ingestion.
- **Data Deletion Workflow**: Includes tools to check and remove content that has been deleted or removed from the source platform, ensuring the dataset remains compliant with right-to-be-forgotten requirements.
- **No Redistribution**: This repository does **not** contain or redistribute real Reddit user data.

## 🚀 Quickstart (Reviewer Demo)

Because this tool requires authenticated API access to collect live data, we provide a **100% synthetic dataset** to allow immediate evaluation of the analysis pipeline.

```bash
# 1. Install dependencies (requires Python 3.11+)
pip install -e .

# 2. Launch the research dashboard
streamlit run app.py

# 3. (In a separate terminal) Ingest the synthetic PPM sample
python scripts/import_external_data.py samples/synthetic_nootropics_sample.csv --acknowledge-pii-scrubbing
```

Once imported, the dashboard will display the `synthetic_nootropics_sample.csv` session, where you can test the **LLM Coder**, **Topic Modeler**, and **PPM Visualization** modules without needing Reddit API credentials.

## ⚙️ Configuration

For full research deployment (live data collection), please refer to the [Technical Setup Guide (SETUP.md)](SETUP.md) for instructions on:

- Acquiring Reddit PRAW credentials.
- Configuring local LLMs via Ollama.
- Managing environment variables.

---

## 📖 Citation

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
