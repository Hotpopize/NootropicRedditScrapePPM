import streamlit as st
import json
import os
from datetime import datetime

st.set_page_config(
    page_title="Academic Research Tool - Nootropics Market Segmentation",
    page_icon="🔬",
    layout="wide"
)

if 'session_id' not in st.session_state:
    st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

if 'collected_data' not in st.session_state:
    st.session_state.collected_data = []

if 'coded_data' not in st.session_state:
    st.session_state.coded_data = []

if 'codebook' not in st.session_state:
    st.session_state.codebook = {
        'push_factors': [],
        'pull_factors': [],
        'mooring_factors': [],
        'emergent_themes': []
    }

st.title("🔬 Academic Research Tool")
st.subheader("Natural Cognitive Supplement Market Segmentation via Reddit Analysis")

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    ["📊 Dashboard", "🌐 Reddit Data Collection", "🤖 LLM-Assisted Coding", "📖 Codebook Management", "💾 Data Export & Audit"]
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
elif page == "💾 Data Export & Audit":
    from modules import data_manager
    data_manager.render()
