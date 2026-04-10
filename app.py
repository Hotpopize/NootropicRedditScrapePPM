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
# Compliance: Reddit Research Data Addendum (executed 2026-04-01)
# ---------------------------------------------------------------------------
# This module implements the hard-fail startup credential gate (§1).
# See COMPLIANCE.md in the repository root for the full mapping.
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
from services.reddit_service import DEFAULT_USER_AGENT
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
# STARTUP GATE: REDDIT AUTHENTICATION
# ---------------------------------------------------------------------------
import os
from dotenv import load_dotenv

load_dotenv()
_client_id = os.getenv("REDDIT_CLIENT_ID")
_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
_user_agent = os.getenv("REDDIT_USER_AGENT", DEFAULT_USER_AGENT)

def _verify_praw_credentials(client_id, client_secret, user_agent):
    if not client_id or not client_secret:
        return False, "Missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET in .env"
    try:
        import praw
        import prawcore
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        reddit.read_only = True
        _ = reddit.user.me() if not reddit.read_only else reddit.random_subreddit()
        return True, ""
    except Exception as e:
        return False, str(e)

if 'reddit_auth_ok' not in st.session_state:
    st.session_state.reddit_auth_ok, st.session_state.reddit_auth_err = _verify_praw_credentials(
        _client_id, _client_secret, _user_agent
    )

if not st.session_state.reddit_auth_ok:
    st.title("🔬 NootropicRedditScrapePPM")
    st.error(
        "🚨 **Reddit API credentials required.**\n\n"
        "This application cannot start without authenticated Reddit API access.\n"
        "Please provide valid developer credentials in your `.env` file.\n\n"
        f"**Error Details:** {st.session_state.reddit_auth_err}\n\n"
        "See `SETUP.md` for the Reddit4Researcher application process and instructions."
    )
    st.stop()

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
        "📖 Codebook Management",
        "🤖 Automated Qualitative Coding",
        "💾 Data Export & Audit",
    ],
)

# Deferred Ollama Status Badge — only shows after first check in Coding module
_ollama_status = st.session_state.get('ollama_status')
if _ollama_status:
    if not _ollama_status['ollama_running']:
        st.sidebar.error("⚠️ Ollama not running")
    elif _ollama_status['missing_models']:
        st.sidebar.warning(f"⚠️ Missing models: {', '.join(_ollama_status['missing_models'])}")
    else:
        st.sidebar.success("✅ Ollama Ready")

# Session info — always visible in sidebar
st.sidebar.divider()
_label = st.session_state.get('session_label', '')
_session_display = _label if _label else st.session_state.session_id
_collected_n = len(st.session_state.get('collected_data', []))
_coded_n = len(st.session_state.get('coded_data', []))
st.sidebar.caption(
    f"Session: **{_session_display}**  \n"
    f"Collected: {_collected_n} items  \n"
    f"Coded: {_coded_n} items"
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
