# logging must be configured BEFORE any project imports so all modules
# inherit the root handler. basicConfig() only activates if the root
# logger has no handlers — calling it after imports risks a no-op.
import logging
logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

# app.py — NootropicRedditScrapePPM entry point
# ===============================================
# Streamlit application for qualitative netnographic analysis of Reddit
# communities using the Push-Pull-Mooring (PPM) framework.
#
# Startup sequence
# ----------------
# 1. Configure logging (before any imports)
# 2. Configure Streamlit page
# 3. init_db() — create/migrate tables, safe on every restart
# 4. Load all data into session_state (once per session)
# 5. Validate Ollama models (cached, TTL 60s)
# 6. Render navigation and route to active module
#
# Session state keys initialised here (available to all modules)
# --------------------------------------------------------------
#   session_id      str      — timestamp-based unique ID for this session
#   db_loaded       bool     — True if initial DB load succeeded
#   collected_data  list     — raw collected posts (from collected_data table)
#   coded_data      list     — PPM coding results (from coded_data table)
#   codebook_manager CodebookManager — loaded codebook
#   ollama_status   dict     — cached validate_models() result
#   ollama_last_check float  — timestamp of last Ollama check (TTL guard)
#
# Keys initialised by modules (not here — lazy init on first access)
# ------------------------------------------------------------------
#   scraping_job_id       — reddit_scraper.py
#   data_source_preference — reddit_scraper.py
#   active_data_source    — reddit_scraper.py
#   active_hash           — reddit_scraper.py
#   collection_runs       — reddit_scraper.py
#   reddit_credentials    — reddit_scraper.py
#   selected_zotero_keywords — reddit_scraper.py
#   zotero_search_query   — reddit_scraper.py
#   emergent_queue        — codebook.py

import time
from datetime import datetime

import streamlit as st

from core.database import init_db
from modules.codebook import CodebookManager
from utils.db_helpers import load_codebook, load_coded_data, load_collected_data
from utils.model_setup import validate_models

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title = "NootropicRedditScrapePPM",
    page_icon  = "🔬",
    layout     = "wide",
)

# ---------------------------------------------------------------------------
# Database initialisation — runs on every cold start, safe to re-run
# ---------------------------------------------------------------------------

try:
    init_db()
except Exception as _db_init_error:
    st.error(
        f"**Database initialisation failed.** The app cannot start.\n\n"
        f"Error: `{_db_init_error}`\n\n"
        f"**What to try:**\n"
        f"- Check that the `data/` directory exists and is writable\n"
        f"- Ensure no other process has the database locked\n"
        f"- Restart the app with `streamlit run app.py`"
    )
    st.stop()   # halt rendering — no point showing the app with a broken DB

# ---------------------------------------------------------------------------
# Session initialisation — runs once per session (not on every rerun)
# ---------------------------------------------------------------------------

if 'session_id' not in st.session_state:
    st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

if 'db_loaded' not in st.session_state:
    try:
        all_collected = load_collected_data(session_id=None, limit=10000)
        all_coded     = load_coded_data(session_id=None, limit=10000)
        all_codebook  = load_codebook(session_id=None)

        st.session_state.collected_data  = all_collected
        st.session_state.coded_data      = all_coded
        st.session_state.codebook_manager = CodebookManager.from_dict(all_codebook)
        st.session_state.db_loaded       = True

    except Exception as _load_error:
        logging.error("Failed to load data from DB on startup: %s", _load_error)
        st.session_state.collected_data   = []
        st.session_state.coded_data       = []
        st.session_state.codebook_manager = CodebookManager()
        st.session_state.db_loaded        = False

# Surface DB load failure as a persistent warning banner (not a crash)
if not st.session_state.get('db_loaded', False):
    st.warning(
        "⚠️ **Could not load existing data from the database.** "
        "Previously collected and coded data may not be visible. "
        "New collections will still be saved correctly. "
        "Try refreshing the page if this persists, check the `data/` directory."
    )

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🔬 NootropicRedditScrapePPM")
st.subheader("Natural Cognitive Supplement Market Segmentation via Reddit Analysis")

# ---------------------------------------------------------------------------
# Sidebar — navigation and system status
# ---------------------------------------------------------------------------

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    [
        "📊 Dashboard",
        "🌐 Data Collection",
        "🤖 Automated Qualitative Coding",
        "📖 Codebook Management",
        "💾 Data Export & Audit",
    ],
)

# Ollama model validation — cached with 60-second TTL so the HTTP check
# does not fire on every st.rerun() (which happens every ~1s during collection).
# validate_models() contacts localhost:11434 — calling it per-rerun adds
# ~2s latency per second during active data collection.
_now           = time.time()
_last_check    = st.session_state.get('ollama_last_check', 0.0)
_OLLAMA_TTL    = 60.0   # seconds between Ollama health checks

with st.sidebar:
    if _now - _last_check > _OLLAMA_TTL or 'ollama_status' not in st.session_state:
        with st.status("Checking System...", expanded=False) as _status:
            _report = validate_models(auto_pull=False)
            st.session_state.ollama_status     = _report
            st.session_state.ollama_last_check = _now

            if not _report['ollama_running']:
                _status.update(label="Ollama Not Found", state="error", expanded=True)
                st.error("Ollama is not running. Automated coding features will be disabled.")
            elif _report['missing_models']:
                _status.update(label="Missing Models", state="warning", expanded=True)
                st.warning(f"Missing models: {', '.join(_report['missing_models'])}")
                if st.button("Install Missing Models"):
                    with st.spinner("Pulling models..."):
                        _pull_report = validate_models(auto_pull=True)
                        if _pull_report['status'] == 'ok':
                            st.success("Models installed!")
                            # Invalidate cache so next render re-checks
                            st.session_state.ollama_last_check = 0.0
                            st.rerun()
                        else:
                            st.error("Failed to install some models.")
            else:
                _status.update(label="System Ready", state="complete", expanded=False)
    else:
        # Cached result — show status without re-checking
        _cached = st.session_state.ollama_status
        if not _cached['ollama_running']:
            st.sidebar.error("⚠️ Ollama not running")
        elif _cached['missing_models']:
            st.sidebar.warning(f"⚠️ Missing models: {', '.join(_cached['missing_models'])}")
        else:
            st.sidebar.success("✅ System Ready")

    # Session info — always visible in sidebar
    st.sidebar.divider()
    st.sidebar.caption(
        f"Session: `{st.session_state.session_id}`  \n"
        f"Collected: {len(st.session_state.get('collected_data', []))} items  \n"
        f"Coded: {len(st.session_state.get('coded_data', []))} items"
    )

# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------

if page == "📊 Dashboard":
    from modules import dashboard
    dashboard.render()

elif page == "🌐 Data Collection":
    from modules import reddit_scraper
    reddit_scraper.render()

elif page == "🤖 Automated Qualitative Coding":
    from modules import llm_coder
    llm_coder.render()

elif page == "📖 Codebook Management":
    from modules import codebook
    codebook.render()

elif page == "💾 Data Export & Audit":
    from modules import data_manager
    data_manager.render()

else:
    # Defensive fallback — should never be reached with a fixed radio widget
    st.error(f"Unknown page: `{page}`. Please select a module from the sidebar.")
