# Project Status & TODOs

## 📌 Current Status

- **Core Functionality**: ✅ Reddit scraping, Automated Qualitative Coding, and Dashboard visualization are implemented.
- **Documentation**: ✅ Initial `README.md` and `CITATION.cff` compliant with academic standards.
- **Dependencies**: ✅ Managed via `pyproject.toml`.

## 📋 To-Do List

### 📝 Documentation & Methodology

- [x] Create `README.md` with setup instructions.
- [ ] Document specific computational prompt strategies in `docs/`.
- [x] Create a user guide for non-technical researchers.

### 🧪 Testing & Reliability

- [ ] **Dependency Verification**: Ensure all typical installs (`pyproject.toml`) work on a fresh env.
- [ ] **End-to-End Test**: Verify Reddit -> Database -> Automated Coding -> Dashboard flow.
- [ ] Add unit tests for `modules/` (especially `llm_coder.py` and `reddit_scraper.py`).
- [ ] Verify database schema consistency during migration/updates.

### ✨ Feature Enhancements

- [x] **LLM Integration**: Allow selection of different Ollama models via UI.
- [ ] **Data Sources**: Add support for other sources beyond Reddit (e.g., specialized forums).
- [ ] **Dashboard**: Add exportable high-resolution charts for publication.

### 🔧 Maintenance

- [x] **Dependencies**: Updates `pyproject.toml` with missing libs (`requests`, `sklearn`, `pyzotero`).
- [ ] Audit dependencies for security vulnerabilities.
- [ ] Refactor `app.py` if simple routing becomes too complex.
