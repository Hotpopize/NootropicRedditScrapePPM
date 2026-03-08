import streamlit as st
import pandas as pd
from datetime import datetime

def render():
    st.header("📊 Research Dashboard")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Posts Collected", len(st.session_state.collected_data))
    
    with col2:
        coded_count = len(st.session_state.coded_data)
        st.metric("Items Coded", coded_count)
    
    with col3:
        total_codes = len(st.session_state.codebook_manager.get_all())
        st.metric("Total Codes in Codebook", total_codes)
    
    with col4:
        completion_rate = (coded_count / len(st.session_state.collected_data) * 100) if st.session_state.collected_data else 0
        st.metric("Coding Progress", f"{completion_rate:.1f}%")
    
    st.divider()
    
    st.subheader("📚 Research Context")
    st.info("""
    **Thesis Focus:** Natural Cognitive Supplement Market Segmentation
    
    **Theoretical Framework:** Push-Pull-Mooring (PPM) Model
    - **Push Factors:** Dissatisfaction with current state (e.g., cognitive decline, stress)
    - **Pull Factors:** Attraction to natural nootropics (e.g., perceived benefits, natural ingredients)
    - **Mooring Factors:** Anchoring elements (e.g., cost, accessibility, trust in sources)
    
    **Data Source:** Reddit communities discussing cognitive enhancement and natural supplements
    
    **Methodology:** Mixed methods following Creswell & Creswell (2023)
    - Thematic analysis of posts and comments
    - Automated qualitative coding with human oversight
    - Export to NVivo/MAXQDA for advanced analysis
    """)
    
    st.divider()
    
    if st.session_state.collected_data:
        st.subheader("📈 Data Collection Summary")
        
        df = pd.DataFrame(st.session_state.collected_data)
        
        st.write(f"**Total Records:** {len(df)}")
        
        if 'subreddit' in df.columns:
            st.write("**Subreddits:**")
            subreddit_counts = df['subreddit'].value_counts()
            st.dataframe(subreddit_counts, use_container_width=True)
        
        if 'created_utc' in df.columns:
            df['date'] = pd.to_datetime(df['created_utc'], unit='s')
            st.write(f"**Date Range:** {df['date'].min().date()} to {df['date'].max().date()}")
    
    if st.session_state.coded_data:
        st.subheader("🎯 Coding Distribution")
        
        coded_df = pd.DataFrame(st.session_state.coded_data)
        
        if 'ppm_category' in coded_df.columns:
            category_counts = coded_df['ppm_category'].value_counts()
            st.bar_chart(category_counts)
        
        if 'themes' in coded_df.columns:
            all_themes = []
            for themes in coded_df['themes']:
                if isinstance(themes, list):
                    all_themes.extend(themes)
            
            if all_themes:
                theme_counts = pd.Series(all_themes).value_counts().head(10)
                st.write("**Top 10 Emergent Themes:**")
                st.dataframe(theme_counts, use_container_width=True)
    
    st.divider()
    
    st.subheader("📋 Session Information")
    col1, col2 = st.columns(2)
    with col1:
        st.write(f"**Session ID:** `{st.session_state.session_id}`")
    with col2:
        st.write(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.caption("All data is stored in session state and can be exported for external analysis.")
