# Project Status & TODOs

## 📌 Current Status

- **Core Functionality**: ✅ Reddit scraping, Automated Qualitative Coding, and Dashboard visualization are implemented.
- **Documentation**: ✅ Initial `README.md` and `CITATION.cff` compliant with academic standards.
- **Dependencies**: ✅ Managed via `pyproject.toml`.

## 📋 To-Do List

### 📝 Documentation & Methodology

- [x] Create `README.md` with setup instructions.
- [x] Document specific computational prompt strategies (implemented in `modules/llm_coder.py`).
- [x] Create a user guide for non-technical researchers.

### 🧪 Testing & Reliability

- [x] **Dependency Verification**: Ensure all typical installs (`pyproject.toml`) work on a fresh env.
- [x] **End-to-End Test**: Verify Reddit -> Database -> Automated Coding -> Dashboard flow (Smoke test).
- [ ] Add unit tests for `modules/` (especially `llm_coder.py` and `reddit_scraper.py`).
- [x] Verify database schema consistency during migration/updates (refactor `init_db`).

### ✨ Feature Enhancements

- [x] **LLM Integration**: Allow selection of different Ollama models via UI.
- [ ] **Data Sources**: Add support for other sources beyond Reddit (e.g., specialized forums).
- [ ] **Dashboard**: Add exportable high-resolution charts for publication.

### 🔧 Maintenance

- [x] **Dependencies**: Updates `pyproject.toml` with missing libs (`requests`, `sklearn`, `pyzotero`).
- [ ] Audit dependencies for security vulnerabilities.
- [ ] Refactor `app.py` if simple routing becomes too complex.

### 🐛 Backlog

- [ ] **MOOR-F ID Format**: `generate_mock_ppm_data.py` must use canonical `MOOR-F-01` format if/when MOOR codes are added.
- [ ] **MOOR-F Placeholders**: Maintain two placeholder slots for MOOR-F if needed.
- [ ] **Scalability Limit**: `app.py` has a 10,000 item limit for `load_collected_data` at startup.

---

## ✅ Done in March 2026

- **Fix / Session Metadata Persistence**: Session labels now persist in the database's `extra_metadata` field upon creation. (Fixes B5/B6)
- **UI / Onboarding Banner**: Added a welcome banner to the dashboard for first-time users (zero data state).
- **UI / Research Context Collapse**: Moved the static PPM framework description into a collapsible expander to declutter the dashboard.
- **UI / Sidebar Session Status**: The active session label (or timestamp) is now pinned to the sidebar for better researcher awareness.
- **Audit / Session Actions**: Rewrote the session action logic to query from the `ScrapeRun` table; added functional Rename and Delete. (Fixes B3/B4)
- **Methodology / GPT-5 Reference**: Removed stale references to GPT-5 in the methodology export; updated to reflect local Ollama (llama3.1/gemma3) processing.

## ✅ Done in April 2026

- **Security / Job Manager**: Implemented `threading.Lock` and active job count validation in `services/job_manager.py` to prevent Background Job Manager Collision race conditions.
- **Security / DB Concurrency**: Increased SQLite timeout in `core/database.py` to 15 seconds to gracefully handle batch LLM processing write locks.
- **Security / Thread Isolation**: Refactored `praw.Reddit` instantiation into the `collect_data` generator for safe thread isolation, and introduced defensive UPSERT (`ON CONFLICT DO NOTHING`) with an `allow_wipe` codebook safety mechanism in `utils/db_helpers.py` to prevent data corruption.
