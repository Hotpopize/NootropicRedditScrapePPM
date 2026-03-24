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

### 🐛 Identified Pre-existing Bugs (Backlog)

- [x] **B3 / Audit Log Disconnect**: Audit trail UI (`modules/data_manager.py`) reads from file, but data action logs are written to DB. Need to rewrite audit tab to query AuditLog table.
- [x] **B4 / Duplicate Button**: Duplicate "Clear All Session Data" button in `modules/data_manager.py` (latent crash).
- [x] **B5 / Schema Gap**: `params.job_id` / `params.session_id` are not natively in `CollectionParams` schema (`services/job_manager.py`). 
- [x] **B6 / Audit Gap**: No `log_action` for job success explicitly captured in `services/job_manager.py`.
- [ ] **MOOR-F ID Format**: `generate_mock_ppm_data.py` must use canonical `MOOR-F-01` format if/when MOOR codes are added.
- [ ] **MOOR-F Placeholders**: Maintain two placeholder slots for MOOR-F if needed.
- [ ] **Scalability Limit**: `app.py` has a 10,000 item limit for `load_collected_data` at startup. (Not an issue for thesis, but note for future).
