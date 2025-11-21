import streamlit as st
import json
import os
from datetime import datetime
from database import init_db
from utils.db_helpers import load_collected_data, load_coded_data, load_codebook

st.set_page_config(
    page_title="Academic Research Tool - Nootropics Market Segmentation",
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
        st.session_state.codebook = all_codebook
        st.session_state.db_loaded = True
    except Exception as e:
        st.session_state.collected_data = []
        st.session_state.coded_data = []
        st.session_state.codebook = {
            'push_factors': [],
            'pull_factors': [],
            'mooring_factors': [],
            'emergent_themes': []
        }
        st.session_state.db_loaded = False

st.title("🔬 Academic Research Tool")
st.subheader("Natural Cognitive Supplement Market Segmentation via Reddit Analysis")

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    ["📊 Dashboard", "🌐 Reddit Data Collection", "🤖 LLM-Assisted Coding", "📖 Codebook Management", "🔍 Topic Modeling", "📈 Inter-Coder Reliability", "📝 Thesis Exports", "💾 Data Export & Audit"]
)

if page == "📊 Dashboard":
    from modules import dashboard
    dashboard.render()
elif page == "🌐 Reddit Data Collection":
    from modules import reddit_scraper
    reddit_scraper.render()
elif page == "🤖 LLM-Assisted Coding":
    from modules import llm_coder
    llm_coder.render()
elif page == "📖 Codebook Management":
    from modules import codebook
    codebook.render()
elif page == "🔍 Topic Modeling":
    from modules import topic_modeling
    topic_modeling.render()
elif page == "📈 Inter-Coder Reliability":
    from modules import reliability
    reliability.render()
elif page == "📝 Thesis Exports":
    from modules import thesis_export
    thesis_export.render()
elif page == "💾 Data Export & Audit":
    from modules import data_manager
    data_manager.render()
