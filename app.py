import streamlit as st
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
from core.database import init_db
from utils.db_helpers import load_collected_data, load_coded_data, load_codebook
from utils.model_setup import validate_models
from modules.codebook import CodebookManager

st.set_page_config(
    page_title="NootropicRedditScrapePPM - Market Segmentation",
    page_icon="🔬",
    layout="wide"
)


init_db()

if 'session_id' not in st.session_state:
    st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

if 'db_loaded' not in st.session_state:
    try:
        all_collected = load_collected_data(session_id=None, limit=10000)
        all_coded = load_coded_data(session_id=None, limit=10000)
        all_codebook = load_codebook(session_id=None)
        
        st.session_state.collected_data = all_collected
        st.session_state.coded_data = all_coded
        st.session_state.codebook_manager = CodebookManager.from_dict(all_codebook)
        st.session_state.db_loaded = True
    except Exception as e:
        st.session_state.collected_data = []
        st.session_state.coded_data = []
        st.session_state.codebook_manager = CodebookManager()
        st.session_state.db_loaded = False

st.title("🔬 NootropicRedditScrapePPM")
st.subheader("Natural Cognitive Supplement Market Segmentation via Reddit Analysis")

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    [
        "📊 Dashboard",
        "🌐 Data Collection",
        "🤖 Automated Qualitative Coding",
        "📖 Codebook Management",
        "💾 Data Export & Audit"
    ]
)
# --- Model Validation ---
with st.sidebar:
    with st.status("Checking System...", expanded=False) as status:
        report = validate_models(auto_pull=False)
        
        if not report['ollama_running']:
            status.update(label="Ollama Not Found", state="error", expanded=True)
            st.error("Ollama is not running. Automated coding features will be disabled.")
        elif report['missing_models']:
            status.update(label="Missing Models", state="warning", expanded=True)
            st.warning(f"Missing models: {', '.join(report['missing_models'])}")
            if st.button("Install Missing Models"):
                 with st.spinner("Pulling models..."):
                     pull_report = validate_models(auto_pull=True)
                     if pull_report['status'] == 'ok':
                         st.success("Models installed!")
                         st.rerun()
                     else:
                         st.error("Failed to install some models.")
        else:
            status.update(label="System Ready", state="complete", expanded=False)

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
