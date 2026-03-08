# Netnographic Protocol & Development Roadmap

**Tool Status**: Alpha (MVP) - Functional Core
**Research Paradigm**: Mixed-Methods Digital Ethnography
**Framework**: Push-Pull-Mooring (PPM) + Emergent Thematic Analysis

---

## 📍 Where We Are (The "MVP" Instrument)

Our current tool, **NootropicRedditScrapePPM**, is a specialized instrument designed for **Passive Observational Netnography**. It has been stripped of "bloat" to focus on a high-fidelity data pipeline.

### 1. Data Collection (The Field Site)

* **Current State**: Automated extraction from `r/Nootropics` via PRAW.
* **Methodological Rule**: "Passive Observation" - We do not interact, post, or influence the community. We capture organic discussions in their natural habitat.
* **Technical**: SQLite storage with WAL mode ensures we capture high-velocity discussions without data loss.

### 2. Data Analysis (The Coding)

* **Current State**: **Automated Qualitative Coding** using Llama 3 (via Ollama).
* **Methodological Rule**: "Privacy & Replicability First".
  * **Privacy**: No participant data is sent to cloud corporate APIs (OpenAI/Anthropic).
  * **Replicability**: Using a static local model version allows precise reproduction of results, unlike shifting cloud models.
* **Logic**: Strict **Deductive Coding** (PPM Framework) combined with **Inductive Coding** (Emergent Themes).
* **Safety**: Circuit breakers prevent infinite looping on malformed data, ensuring the integrity of the coding process.

### 3. Usability

* **Current State**: Single-user Streamlit dashboard.
* **Function**: "One-click" pipeline from Scraping -> Database -> Coding -> Visualization.

---

## 🗺️ Where It Needs To Go (The Roadmap)

To transition from a "working tool" to a "publication-grade academic platform," the following tiers must be developed.

### Tier 1: Methodological Rigor & Validation (Short Term)

* **Audit Trails**: Every single automated coding decision (input prompt + output JSON) must be logged to an immutable `AuditLog` table.
  * *Why*: To defend against accusations of "AI Hallucination" during peer review.
* **Inter-Coder Reliability (ICR)**: We need a mode where *human* researchers code 5% of the data, and the system calculates a Kappa score against the AI's coding.
  * *Why*: To valid statistical alignment between human theory and machine execution.

### Tier 2: Contextual Depth (Medium Term)

* **Author Profiling**: Tracking specific "Power Users" over time (longitudinal study) rather than just isolated posts.
  * *Why*: To understand the *journey* of a user from "curious" to "expert" to "quitting".
* **Sentiment Granularity**: Adding distinct sentiment layers (e.g., "Frustrated" vs "Hopeful") attached to specific PPM factors.

### Tier 3: Dissemination & Ethics (Long Term)

* **Anonymization Export**: A "Safe Export" feature that scrubs usernames/IDs but preserves the semantic content for public dataset sharing (OSF/Zenodo).
* **"One-Click" Setup**: The `setup_guide.py` (currently planned) needs to be fully implemented so non-technical sociologists can install the tool.

---

## 📜 The "Golden Rules" of NootropicRedditScrapePPM

1. The tool remains invisible to Reddit users.
2. Data never leaves the researcher's machine.
3. Automated coding acts as an assistant, not the author. All outputs are subject to human audit.
4.link coded snippets back to the full original thread.
