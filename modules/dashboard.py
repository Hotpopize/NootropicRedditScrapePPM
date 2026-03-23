# modules/dashboard.py
# =====================
# Research metrics dashboard for the NootropicRedditScrapePPM thesis tool.
#
# Purpose
# -------
# Provides at-a-glance visibility into data collection progress, coding
# distribution, and session status. Reads from session_state only — no DB
# writes, no background calls.
#
# All four session_state keys accessed here (collected_data, coded_data,
# codebook_manager, session_id) are guaranteed by app.py's startup
# initialisation block and will never be absent.
#
# Thesis context
# --------------
# Title:     "Caffeine to Brain Boosts: Using Online Communities to Understand
#             the Nootropics Market"
# Framework: Push-Pull-Mooring (PPM), applied deductively via netnographic
#            analysis of five Reddit communities.
# Target:    150–200 posts across r/Nootropics, r/StackAdvice, r/Supplements,
#            r/Decaf, r/Biohackers (2020–2025).
#
# Exported symbols
# ----------------
#   render() — Streamlit page entry point (called by app.py)

from datetime import datetime

import pandas as pd
import streamlit as st

# Thesis collection targets — methodology constants, not UI settings.
# Defined here to match the 150-200 post target in Chapter 3.
THESIS_TARGET_MIN = 150
THESIS_TARGET_MAX = 200


def render() -> None:
    st.header("📊 Research Dashboard")

    # --- DB health warning ---
    # db_loaded is set False by app.py if init_db() failed.
    # Metrics will show zeros but the warning makes the cause explicit.
    if not st.session_state.get('db_loaded', True):
        st.warning(
            "⚠️ Database did not initialise correctly. "
            "Metrics below reflect in-memory session data only and may be incomplete."
        )

    # -----------------------------------------------------------------------
    # Top metrics row
    # -----------------------------------------------------------------------
    collected_count = len(st.session_state.collected_data)
    coded_count     = len(st.session_state.coded_data)
    total_codes     = len(st.session_state.codebook_manager.get_all())
    completion_pct  = (coded_count / collected_count * 100) if collected_count else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Posts Collected", collected_count)
    with col2:
        st.metric("Items Coded", coded_count)
    with col3:
        st.metric("Codebook Entries", total_codes)
    with col4:
        st.metric("Coding Progress", f"{completion_pct:.1f}%")

    st.divider()

    # -----------------------------------------------------------------------
    # Thesis target progress
    # -----------------------------------------------------------------------
    st.subheader("🎯 Collection Progress vs Thesis Target")

    target_mid  = (THESIS_TARGET_MIN + THESIS_TARGET_MAX) / 2
    progress_val = min(collected_count / THESIS_TARGET_MAX, 1.0)
    st.progress(progress_val)

    target_col1, target_col2, target_col3 = st.columns(3)
    with target_col1:
        st.metric("Collected", collected_count)
    with target_col2:
        st.metric("Target Range", f"{THESIS_TARGET_MIN}–{THESIS_TARGET_MAX}")
    with target_col3:
        remaining = max(THESIS_TARGET_MIN - collected_count, 0)
        st.metric(
            "Remaining to Minimum",
            remaining,
            delta=f"–{remaining}" if remaining > 0 else "✓ Target met",
            delta_color="inverse" if remaining > 0 else "normal",
        )

    st.divider()

    # -----------------------------------------------------------------------
    # Research Context
    # -----------------------------------------------------------------------
    st.subheader("📚 Research Context")
    st.info(
        "**Thesis:** Caffeine to Brain Boosts: Using Online Communities to Understand "
        "the Nootropics Market\n\n"
        "**Theoretical Framework:** Push-Pull-Mooring (PPM) model — applied deductively "
        "via netnographic analysis of Reddit communities.\n"
        "- **Push Factors (7 codes):** Dissatisfaction with conventional stimulants "
        "(side effects, tolerance, health risk, dependency, cost, ethics, efficacy doubt)\n"
        "- **Pull Factors (7 codes):** Attraction to natural nootropics "
        "(naturalness, safety, sustainability, holistic benefits, community endorsement, "
        "neuroprotection, cognitive specificity)\n"
        "- **Mooring Factors (12 codes):** Barriers and facilitators of switching "
        "(habit, financial costs, learning costs, information asymmetry, stigma, "
        "ethics; community info, accessibility, health consciousness, low switching costs)\n\n"
        "**Communities:** r/Nootropics · r/StackAdvice · r/Supplements · "
        "r/Decaf · r/Biohackers (2020–2025)\n\n"
        "**Method:** Qualitative netnography with deductive PPM coding via local LLM "
        "(Ollama: llama3.1 / gemma3:12b). Export to NVivo/MAXQDA for deep analysis."
    )

    st.divider()

    # -----------------------------------------------------------------------
    # Data Collection Summary
    # -----------------------------------------------------------------------
    if st.session_state.collected_data:
        st.subheader("📈 Collection Summary")

        df = pd.DataFrame(st.session_state.collected_data)
        st.write(f"**Total Records:** {len(df)}")

        col_left, col_right = st.columns(2)

        with col_left:
            if 'subreddit' in df.columns:
                st.write("**Posts by Subreddit**")
                subreddit_counts = df['subreddit'].value_counts()
                st.bar_chart(subreddit_counts)

        with col_right:
            if 'data_source' in df.columns:
                st.write("**Collection Method**")
                source_counts = df['data_source'].value_counts().rename({
                    'praw':          'Reddit API (PRAW)',
                    'json_endpoint': 'JSON Endpoint',
                })
                st.bar_chart(source_counts)

        if 'created_utc' in df.columns:
            # Filter out zero/null timestamps before conversion to avoid
            # 1970-01-01 appearing as the date range minimum.
            valid_utc = df['created_utc'].dropna()
            valid_utc = valid_utc[valid_utc > 0]
            if not valid_utc.empty:
                dates = pd.to_datetime(valid_utc, unit='s', errors='coerce').dropna()
                if not dates.empty:
                    st.write(
                        f"**Date Range:** {dates.min().date()} — {dates.max().date()}"
                    )

    # -----------------------------------------------------------------------
    # Coding Distribution
    # -----------------------------------------------------------------------
    if st.session_state.coded_data:
        st.divider()
        st.subheader("🎯 Coding Distribution")

        coded_df = pd.DataFrame(st.session_state.coded_data)

        row1_left, row1_right = st.columns(2)

        with row1_left:
            if 'ppm_category' in coded_df.columns:
                st.write("**PPM Category Distribution**")
                cat_counts = coded_df['ppm_category'].value_counts()
                st.bar_chart(cat_counts)

        with row1_right:
            if 'confidence' in coded_df.columns:
                st.write("**Coding Confidence Distribution**")
                conf_counts = coded_df['confidence'].value_counts()
                st.bar_chart(conf_counts)

        # Per-subcode breakdown — primary analytical output for thesis Chapter 4
        if 'ppm_subcodes' in coded_df.columns:
            st.write("**Subcode Frequency (top 15)**")
            all_subcodes = []
            for subcodes in coded_df['ppm_subcodes']:
                if isinstance(subcodes, list):
                    all_subcodes.extend(subcodes)
            if all_subcodes:
                subcode_counts = (
                    pd.Series(all_subcodes)
                    .value_counts()
                    .head(15)
                )
                st.bar_chart(subcode_counts)
            else:
                st.caption("No subcodes assigned yet.")

        # Emergent themes
        if 'themes' in coded_df.columns:
            all_themes = []
            for themes in coded_df['themes']:
                if isinstance(themes, list):
                    all_themes.extend(themes)
            if all_themes:
                st.write("**Top 10 Emergent Themes**")
                theme_counts = pd.Series(all_themes).value_counts().head(10)
                st.dataframe(theme_counts, use_container_width=True)

    # -----------------------------------------------------------------------
    # Session Information
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("📋 Session Information")

    col1, col2 = st.columns(2)
    with col1:
        st.write(f"**Session ID:** `{st.session_state.session_id}`")
    with col2:
        st.write(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.caption(
        "All data is persisted to the local SQLite database. "
        "Use the Data Export module to export for NVivo/MAXQDA analysis."
    )
