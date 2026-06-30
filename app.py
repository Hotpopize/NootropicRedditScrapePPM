import os
import time
import logging
from datetime import datetime
import streamlit as st

from core.database import init_db
from modules.codebook import CodebookManager
from utils.db_helpers import load_codebook, load_coded_data, load_collected_data

# Set up page config
st.set_page_config(
    page_title="NootropicRedditScrapePPM",
    page_icon="🔬",
    layout="wide",
)

# Initialize database
try:
    init_db()
except Exception as e:
    st.error(f"Database initialisation failed: {e}")
    st.stop()

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

if 'db_loaded' not in st.session_state:
    try:
        all_collected = load_collected_data(session_id=None, limit=None)
        all_coded     = load_coded_data(session_id=None, limit=None)
        all_codebook  = load_codebook(session_id=None)

        st.session_state.collected_data   = all_collected
        st.session_state.coded_data       = all_coded
        st.session_state.codebook_manager = CodebookManager.from_dict(all_codebook)
        st.session_state.db_loaded        = True
    except Exception as e:
        st.session_state.collected_data   = []
        st.session_state.coded_data       = []
        st.session_state.codebook_manager = CodebookManager()
        st.session_state.db_loaded        = False
        st.warning(f"Could not load existing data: {e}")

# Header
st.title("🔬 NootropicRedditScrapePPM")
st.subheader("Natural Cognitive Supplement Market Segmentation via Reddit Analysis (Merged Database)")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    [
        "📊 Dashboard",
        "📖 Codebook Management",
        "💾 Data Export & Audit",
        "📄 Thesis Export Templates",
    ],
)

# Session Info in Sidebar
st.sidebar.divider()
collected_n = len(st.session_state.get('collected_data', []))
coded_n = len(st.session_state.get('coded_data', []))
st.sidebar.caption(
    f"Session: **Merged Canonical DB**  \n"
    f"Collected: {collected_n} items  \n"
    f"Coded: {coded_n} items"
)

# Page routing
if page == "📊 Dashboard":
    from modules import dashboard
    dashboard.render()
elif page == "📖 Codebook Management":
    from modules import codebook
    codebook.render()
elif page == "💾 Data Export & Audit":
    from modules import data_manager
    data_manager.render()
elif page == "📄 Thesis Export Templates":
    from modules import thesis_export
    thesis_export.render()
