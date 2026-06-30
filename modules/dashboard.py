# modules/dashboard.py
# =====================
# Research metrics dashboard for the NootropicRedditScrapePPM thesis tool.
#
# Purpose
# -------
# Provides at-a-glance visibility into data collection progress, coding
# distribution, and session status. Reads from session_state by default;
# optionally filters by session using session_id field in item dicts.
# Queries DB once per render for the session list (get_all_sessions).
# Inline session actions (rename, test-flag, delete) are available when
# a specific session is selected — lightweight alternative to the full
# session browser in data_manager.py.
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
#            analysis of six Reddit communities.
# Target:    660 posts across r/Nootropics, r/StackAdvice, r/Supplements,
#            r/Decaf, r/Biohackers, r/NooTopics (2020–2025).
#
# Exported symbols
# ----------------
#   render() — Streamlit page entry point (called by app.py)

from datetime import datetime

import pandas as pd
import streamlit as st

from utils.db_helpers import (
    get_all_sessions,
    update_session_metadata,
    delete_session_data,
    log_action,
    load_collected_data,
    load_coded_data,
)

# Thesis collection targets — methodology constants, not UI settings.
# Defined here to match the 660 post target in Chapter 3.
THESIS_TARGET_MIN = 660
THESIS_TARGET_MAX = 660


def render() -> None:
    st.header("📊 Research Dashboard")

    # --- DB health warning ---
    if not st.session_state.get('db_loaded', True):
        st.warning(
            "⚠️ Database did not initialise correctly. "
            "Metrics below reflect in-memory session data only and may be incomplete."
        )

    # -----------------------------------------------------------------------
    # Session filter — overlay, does not mutate session_state
    # -----------------------------------------------------------------------
    current_session_id = st.session_state.get('session_id', '')
    all_sessions = get_all_sessions()

    # Build filter options: "All sessions" + one entry per session
    session_ids = []          # parallel list for index-based lookup
    session_options = ["All sessions"]

    for s in all_sessions:
        label_part = s.get('label') or 'Unlabelled'
        count_part = f"{s['collected_count']} posts"
        test_badge = ' 🧪' if s.get('is_test') else ''
        active_badge = ' ⚡ ACTIVE' if s['session_id'] == current_session_id else ''
        display = (
            f"{s['session_id']} — {label_part} "
            f"({count_part}){test_badge}{active_badge}"
        )
        session_options.append(display)
        session_ids.append(s['session_id'])

    # Only show the filter if there are sessions to filter
    if all_sessions:
        selected_idx = st.selectbox(
            "Filter by session",
            range(len(session_options)),
            format_func=lambda i: session_options[i],
            key="dashboard_session_filter",
        )
    else:
        selected_idx = 0  # "All sessions" — no filter

    # Apply filter
    if selected_idx == 0:
        # "All sessions" — use everything in session_state
        filtered_collected = st.session_state.collected_data
        filtered_coded     = st.session_state.coded_data

        # Test-run warning: if any session is flagged as test, warn
        has_test_sessions = any(s.get('is_test') for s in all_sessions)
        if has_test_sessions:
            st.warning(
                "⚠️ **Test session data included.** Some sessions are flagged as "
                "test runs. Select a specific session above to exclude test data, "
                "or use the session actions to change the flag."
            )
    else:
        # Specific session selected — filter in Python
        sel_session_id = session_ids[selected_idx - 1]
        sel_session = all_sessions[selected_idx - 1]
        filtered_collected = [
            item for item in st.session_state.collected_data
            if item.get('session_id') == sel_session_id
        ]
        filtered_coded = [
            item for item in st.session_state.coded_data
            if item.get('session_id') == sel_session_id
        ]

        # --- Inline session actions ---
        is_active = sel_session_id == current_session_id
        has_scrape_run = sel_session.get('status') is not None

        with st.expander("📋 Session Actions", expanded=False):

            if has_scrape_run:
                col_lbl, col_tst = st.columns([3, 1])
                with col_lbl:
                    new_label = st.text_input(
                        "Rename",
                        value=sel_session.get('label') or '',
                        placeholder="e.g. Thesis run 1",
                        key="dash_session_label",
                    )
                with col_tst:
                    new_is_test = st.checkbox(
                        "🧪 Test run",
                        value=sel_session.get('is_test', False),
                        key="dash_test_flag",
                    )

                label_changed = (new_label or None) != (sel_session.get('label') or None)
                test_changed  = new_is_test != sel_session.get('is_test', False)

                if label_changed or test_changed:
                    if st.button("💾 Save", key="dash_save_meta"):
                        update_session_metadata(
                            session_id=sel_session_id,
                            label=new_label if label_changed else None,
                            is_test=new_is_test if test_changed else None,
                        )
                        st.success("✅ Updated.")
                        st.rerun()
            else:
                st.caption("ℹ️ Rename/test-flag unavailable for imported sessions.")

            # Delete
            total_to_delete = sel_session['collected_count'] + sel_session['coded_count']

            if is_active:
                st.button(
                    f"🗑️ Delete ({total_to_delete} items)",
                    disabled=True,
                    help="Cannot delete the active session.",
                    key="dash_delete_disabled",
                )
            elif total_to_delete == 0:
                st.caption("No data to delete. Audit trail preserved.")
            else:
                st.warning(
                    f"Permanently delete **{sel_session['collected_count']}** collected "
                    f"and **{sel_session['coded_count']}** coded item(s)?"
                )
                if st.button(
                    f"🗑️ Delete {total_to_delete} Items",
                    type="secondary",
                    key="dash_delete_btn",
                ):
                    try:
                        result = delete_session_data(sel_session_id)
                        log_action(
                            action='session_deleted',
                            session_id=current_session_id,
                            details={
                                'deleted_session_id': sel_session_id,
                                'collected_deleted':  result['collected_deleted'],
                                'coded_deleted':      result['coded_deleted'],
                            },
                        )
                        st.session_state.collected_data = load_collected_data(
                            session_id=None, limit=10000
                        )
                        st.session_state.coded_data = load_coded_data(
                            session_id=None, limit=10000
                        )
                        st.success(
                            f"✅ Deleted {result['collected_deleted']} collected, "
                            f"{result['coded_deleted']} coded."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Delete failed: {e}")

    # -----------------------------------------------------------------------
    # Top metrics row
    # -----------------------------------------------------------------------
    # Reconciled counts: separate submissions (posts) and comments
    # Also exclude test PRAW runs (data_source = 'json_endpoint') from core thesis metrics
    bqah_submissions = [d for d in filtered_collected if d.get('type') == 'submission' and d.get('data_source') != 'json_endpoint']
    bqah_comments    = [d for d in filtered_collected if d.get('type') == 'comment' and d.get('data_source') != 'json_endpoint']
    
    # Map from reddit_id -> (type, data_source) for lookup of coded data attributes
    id_map = {item['id']: (item.get('type'), item.get('data_source')) for item in filtered_collected}
    
    coded_submissions = []
    coded_comments = []
    for d in filtered_coded:
        i_type, i_source = id_map.get(d['id'], (None, None))
        if i_source != 'json_endpoint':
            if i_type == 'submission':
                coded_submissions.append(d)
            elif i_type == 'comment':
                coded_comments.append(d)
    
    n_submissions = len(bqah_submissions)
    n_comments    = len(bqah_comments)
    n_coded_sub   = len(coded_submissions)
    n_coded_comm  = len(coded_comments)
    
    # Calculate non-empty PPM coding counts
    import json
    def parse_subcodes(raw) -> set:
        if not raw:
            return set()
        if isinstance(raw, str):
            try:
                val = json.loads(raw)
            except Exception:
                return set()
        else:
            val = raw
        if not isinstance(val, list):
            return set()
        out = set()
        for item in val:
            if isinstance(item, dict):
                item = item.get("code", "")
            if isinstance(item, str):
                out.add(item.strip().upper())
        return out

    non_empty_sub = sum(1 for d in coded_submissions if len(parse_subcodes(d.get('ppm_subcodes'))) > 0)
    non_empty_comm = sum(1 for d in coded_comments if len(parse_subcodes(d.get('ppm_subcodes'))) > 0)
    
    sub_non_empty_pct = (non_empty_sub / n_submissions * 100) if n_submissions else 0.0
    comm_non_empty_pct = (non_empty_comm / n_comments * 100) if n_comments else 0.0

    total_codes     = len(st.session_state.codebook_manager.get_all())
    completion_pct  = (n_coded_sub / n_submissions * 100) if n_submissions else 0.0

    # --- Onboarding banner for first-time users ---
    if len(filtered_collected) == 0 and len(filtered_coded) == 0 and selected_idx == 0:
        st.info(
            "👋 **Welcome!** You haven't collected any data yet.\n\n"
            "Go to **🌐 Data Collection** in the sidebar to get started. "
            f"Your thesis needs {THESIS_TARGET_MIN}–{THESIS_TARGET_MAX} posts "
            "from 6 subreddits — the default settings are already configured. "
            "Just type a session label, click Start, and wait."
        )

    # Top metrics columns
    col_sub, col_comm = st.columns(2)
    
    with col_sub:
        st.markdown("### 📝 Submissions (Posts)")
        col_s1, col_s2, col_s3 = st.columns(3)
        col_s1.metric("Collected", f"{n_submissions:,}")
        col_s2.metric("Coded", f"{n_coded_sub:,}", "100% complete")
        col_s3.metric("Non-Empty PPM", f"{non_empty_sub:,}", f"{sub_non_empty_pct:.1f}% of total")
        
    with col_comm:
        st.markdown("### 💬 Comments")
        col_c1, col_c2, col_c3 = st.columns(3)
        col_c1.metric("Collected", f"{n_comments:,}")
        col_c2.metric("Coded", f"{n_coded_comm:,}", "100% complete")
        col_c3.metric("Non-Empty PPM", f"{non_empty_comm:,}", f"{comm_non_empty_pct:.1f}% of total")

    st.divider()
    
    col_cb, col_prog = st.columns(2)
    with col_cb:
        st.metric("Codebook Entries", total_codes)
    with col_prog:
        st.metric("Submissions Coding Progress", f"{completion_pct:.1f}%")

    st.divider()

    # -----------------------------------------------------------------------
    # Thesis target progress
    # -----------------------------------------------------------------------
    st.subheader("🎯 Collection Progress vs Thesis Target")

    progress_val_sub = min(n_submissions / 660, 1.0)
    st.markdown("**1. Core Deductive PPM Sample Target (Submissions)**")
    st.progress(progress_val_sub)

    target_col1, target_col2, target_col3 = st.columns(3)
    with target_col1:
        st.metric("Submissions Collected", f"{n_submissions:,}")
    with target_col2:
        st.metric("Deductive PPM Target", "660 posts")
    with target_col3:
        remaining_sub = max(660 - n_submissions, 0)
        st.metric(
            "Remaining Submissions",
            remaining_sub,
            delta=f"–{remaining_sub}" if remaining_sub > 0 else "✓ Target met",
            delta_color="inverse" if remaining_sub > 0 else "normal",
        )

    st.markdown("**2. Full Coded Corpus Target (Total Units)**")
    total_units = n_submissions + n_comments
    progress_val_corpus = min(total_units / 58415, 1.0)
    st.progress(progress_val_corpus)

    corpus_col1, corpus_col2, corpus_col3 = st.columns(3)
    with corpus_col1:
        st.metric("Total Units (Submissions + Comments)", f"{total_units:,}")
    with corpus_col2:
        st.metric("Full Corpus Target", "58,415 BQAH units")
    with corpus_col3:
        remaining_corpus = max(58415 - total_units, 0)
        st.metric(
            "Remaining Units",
            remaining_corpus,
            delta=f"–{remaining_corpus}" if remaining_corpus > 0 else "✓ Target met",
            delta_color="inverse" if remaining_corpus > 0 else "normal",
        )

    st.divider()

    # -----------------------------------------------------------------------
    # Research Context (collapsed — useful for orientation, not daily use)
    # -----------------------------------------------------------------------
    with st.expander("📚 Research Context", expanded=False):
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
            "- **Mooring Factors (10 codes):** Barriers and facilitators of switching "
            "(habit, financial costs, learning costs, information asymmetry, stigma, "
            "ethics; community info, accessibility, health consciousness, low switching costs)\n\n"
            "**Communities:** r/Nootropics · r/StackAdvice · r/Supplements · "
            "r/Decaf · r/Biohackers · r/NooTopics (2020–2025)\n\n"
            "**Method:** Qualitative netnography with deductive PPM coding via local LLM "
            "(Ollama: llama3.1 / gemma3:12b). Export to NVivo/MAXQDA for deep analysis."
        )

    st.divider()

    # -----------------------------------------------------------------------
    # Data Collection Summary
    # -----------------------------------------------------------------------
    if filtered_collected:
        st.subheader("📈 Collection Summary")

        df = pd.DataFrame(filtered_collected)
        st.write(f"**Total Records:** {len(df)}")

        if 'created_utc' in df.columns:
            valid_utc = df['created_utc'].dropna()
            valid_utc = valid_utc[valid_utc > 0]
            if not valid_utc.empty:
                dates = pd.to_datetime(valid_utc, unit='s', errors='coerce').dropna()
                if not dates.empty:
                    st.write(
                        f"**Date Range:** {dates.min().date()} — {dates.max().date()}"
                    )

        # --- Per-subreddit temporal matrix ---
        if 'subreddit' in df.columns and 'created_utc' in df.columns:
            valid = df[df['created_utc'].notna() & (df['created_utc'] > 0)].copy()
            if not valid.empty:
                valid['_year'] = pd.to_datetime(
                    valid['created_utc'], unit='s', errors='coerce'
                ).dt.year

                st.write("**Temporal Coverage (posts per subreddit × year)**")
                pivot = valid.pivot_table(
                    index='subreddit', columns='_year',
                    values='id', aggfunc='count', fill_value=0,
                )
                pivot.columns = [str(int(y)) for y in pivot.columns]

                # Style the dataframe to highlight 0 values (gaps)
                def highlight_zeros(val):
                    color = 'red' if val == 0 else ''
                    return f'color: {color}'
                    
                st.dataframe(
                    pivot.style.map(highlight_zeros),
                    use_container_width=True
                )

    # -----------------------------------------------------------------------
    # Coding Distribution
    # -----------------------------------------------------------------------
    if filtered_coded:
        st.divider()
        st.subheader("🎯 Coding Distribution")

        coded_df = pd.DataFrame(filtered_coded)

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
        if selected_idx == 0:
            st.write(f"**Viewing:** All sessions ({len(all_sessions)} total)")
        else:
            sel_label = all_sessions[selected_idx - 1].get('label') or 'Unlabelled'
            st.write(f"**Viewing:** {sel_label} (`{session_ids[selected_idx - 1]}`)")
    with col2:
        st.write(f"**Current Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.caption(
        "All data is persisted to the local SQLite database. "
        "Use the Data Export module to export for NVivo/MAXQDA analysis."
    )
