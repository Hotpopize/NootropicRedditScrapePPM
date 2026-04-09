import streamlit as st
import pandas as pd
from datetime import datetime

from utils.db_helpers import save_collected_data, log_action, save_replicability_log, get_all_zotero_keywords, update_session_metadata

# --- THESIS CONFIGURATION CONSTANTS ---
THESIS_SUBREDDITS = [
    'Nootropics',
    'StackAdvice',
    'Supplements',
    'Decaf',
    'Biohackers'
]


def render():
    st.header("Data Collection")

    # ===================================================================
    # Section 1: Session Label + Mode Badge (always visible)
    # ===================================================================

    session_label = st.text_input(
        "📋 Label this session",
        value=st.session_state.get('session_label', ''),
        placeholder="e.g. Test run, Thesis collection 1, Pilot",
        help="Optional label stored with this collection for easy identification later.",
        key="session_label_input",
    )
    st.session_state.session_label = session_label

    st.caption("🔑 Collecting with Reddit API credentials")

    # ===================================================================
    # Section 2: Thesis summary + Start button
    # ===================================================================

    # The Advanced expander below contains the thesis toggle. We read its
    # value here via session_state so the toggle is inside Advanced but
    # its EFFECT is visible at the top level.
    if 'use_thesis_config' not in st.session_state:
        st.session_state.use_thesis_config = True

    use_thesis_config = st.session_state.use_thesis_config

    if use_thesis_config:
        # --- Thesis mode: methodology-locked parameters ---
        # Subreddits are selectable so the researcher can run one sub at a time
        # for temporal coverage control. All other parameters are locked.
        subreddits = st.multiselect(
            "Thesis subreddits to collect",
            options=THESIS_SUBREDDITS,
            default=THESIS_SUBREDDITS,
            help=(
                "These 5 subreddits represent the core of the nootropics community. "
                "You can deselect specific ones to run a targeted collection."
            ),
            key="thesis_sub_select",
        )
        collection_method = "Top Posts (Time Period)"
        time_filter       = "all"
        limit             = 50
        search_query      = ""
        collect_comments  = False
        comment_limit     = 0
        include_nsfw      = False
        include_removed   = True
        include_media_only = False
        flag_non_english  = True
        max_text_length   = 50000
        min_word_count_val = 20

        st.info(
            "🎓 **Thesis Mode** · "
            f"{len(subreddits)} of {len(THESIS_SUBREDDITS)} subreddits · "
            "Top posts (all time) · "
            f"{limit}/sub · "
            f"min {min_word_count_val} words · "
            "text only"
        )

    # If thesis mode is OFF, parameters are set inside the Advanced
    # expander below. We define them here as None and fill them in the
    # expander. The Start button guard checks `subreddits` after the
    # expander has run.
    else:
        subreddits         = None  # set inside expander
        collection_method  = None
        time_filter        = None
        limit              = None
        search_query       = None
        collect_comments   = None
        comment_limit      = None
        include_nsfw       = None
        include_removed    = None
        include_media_only = None
        flag_non_english   = None
        max_text_length    = None
        min_word_count_val = None

    # ===================================================================
    # Section 3: Advanced Settings (collapsed by default)
    # ===================================================================

    with st.expander("⚙️ Advanced Settings", expanded=not use_thesis_config):

        # --- Thesis toggle (first item) ---
        thesis_checked = st.checkbox(
            "🎓 Use Thesis Configuration (Methodology Standard)",
            value=st.session_state.use_thesis_config,
            help="Locks parameters to thesis methodology: 5 subreddits, "
                 "Top Posts (all time), 50 per sub, min 20 words, text only.",
            key="thesis_config_checkbox",
        )
        st.session_state.use_thesis_config = thesis_checked

        if thesis_checked:
            # --- Thesis mode ON: show locked summary ---
            st.caption(
                f"**Subreddits:** {', '.join(THESIS_SUBREDDITS)}\n\n"
                f"**Method:** Top Posts (all time) · 50 per subreddit\n\n"
                f"**Filters:** Min 20 words · No media-only · No NSFW · No comments"
            )

        else:
            # --- Thesis mode OFF: show all parameter widgets ---
            st.divider()
            st.write("**Collection Parameters**")

            col1, col2 = st.columns(2)

            with col1:
                subreddits_input = st.text_area(
                    "Target Subreddits (one per line)",
                    value="Nootropics\nStackAdvice\nSupplements\nCognitive_Neuroscience",
                    help="Enter subreddit names without r/ prefix",
                    height=150,
                )
                subreddits = [s.strip() for s in subreddits_input.split('\n') if s.strip()]

                default_query = st.session_state.get('zotero_search_query', '')
                search_query = st.text_input(
                    "Search Query (optional)",
                    value=default_query if default_query else '',
                    help="Leave empty to collect by method above, or enter keywords.",
                )

            with col2:
                collection_method = st.selectbox(
                    "Collection Method",
                    ["Recent Posts (Hot)", "Recent Posts (New)",
                     "Top Posts (Time Period)", "Search Query"],
                     help=(
                         "**Hot**: Trending recently.\n\n"
                         "**New**: The most recent posts chronologically.\n\n"
                         "**Top**: Most upvoted in the selected period (best for consensus themes)."
                     )
                )

                if collection_method == "Top Posts (Time Period)":
                    time_filter = st.selectbox(
                        "Time Period",
                        ["day", "week", "month", "year", "all"],
                    )
                else:
                    time_filter = "all"

                limit = st.number_input(
                    "Posts per Subreddit",
                    min_value=1, max_value=1000, value=100,
                    help="Max number of items to fetch per community. Higher limits increase collection time.",
                )

                collect_comments = st.checkbox(
                    "Collect Comments",
                    value=False,
                    help="Collect top comments per post.",
                )

                if collect_comments:
                    comment_limit = st.number_input(
                        "Max Comments per Post",
                        min_value=1, max_value=500, value=50,
                    )
                else:
                    comment_limit = 0

            # --- Content Filtering ---
            st.divider()
            st.write("**Content Filtering**")

            col3, col4 = st.columns(2)

            with col3:
                include_nsfw = st.checkbox(
                    "Include NSFW Content", value=False,
                    help="Include posts/subreddits marked as NSFW (18+)",
                )
                include_removed = st.checkbox(
                    "Include Removed/Deleted Content", value=True,
                    help="Include posts where content is [removed] or [deleted]",
                )
                include_media_only = st.checkbox(
                    "Include Media-Only Posts", value=False,
                    help="Include image/video posts without text content",
                )

            with col4:
                flag_non_english = st.checkbox(
                    "Flag Non-English Content", value=True,
                    help="Detects and flags likely non-English posts for easier exclusion during analysis.",
                )
                max_text_length = st.number_input(
                    "Max Text Length (chars)",
                    min_value=1000, max_value=100000, value=50000,
                    help="Very long posts (e.g. detailed 'stack' guides) will be truncated.",
                )
                min_word_count_val = st.number_input(
                    "Min Word Count",
                    min_value=0, max_value=1000, value=0,
                    help="Filters out short or low-substance posts (e.g., 'thank you').",
                )

            # --- Zotero keywords (only when thesis mode is OFF) ---
            try:
                zotero_keywords = get_all_zotero_keywords()
            except Exception:
                zotero_keywords = []

            if zotero_keywords:
                st.divider()
                st.write("**📚 Zotero-Informed Search Keywords**")

                selected_zotero_kw = st.multiselect(
                    "Select keywords from your literature",
                    options=zotero_keywords,
                    default=st.session_state.get('selected_zotero_keywords', [])[:5],
                    help="Keywords extracted from your synced Zotero references",
                )
                st.session_state.selected_zotero_keywords = selected_zotero_kw

                if selected_zotero_kw:
                    suggested_query = ' OR '.join(selected_zotero_kw[:5])
                    st.info(f"**Suggested search query:** `{suggested_query}`")
                    if st.button("Use as Search Query"):
                        st.session_state.zotero_search_query = suggested_query



    # ===================================================================
    # Start button (outside expander, always visible)
    # ===================================================================

    if 'scraping_job_id' not in st.session_state:
        st.session_state.scraping_job_id = None

    if not subreddits:
        st.warning("⚠️ No subreddits specified. Add at least one subreddit above before collecting data.")

    if st.button(
        "▶ Start Data Collection",
        type="primary",
        disabled=(st.session_state.scraping_job_id is not None or not subreddits),
    ):
        try:
            from core.schemas import RedditCredentials, CollectionParams
            from services.job_manager import JobManager
            
            import os
            from services.reddit_service import RedditService
            creds_obj = RedditCredentials(
                client_id=os.environ.get('REDDIT_CLIENT_ID', ''),
                client_secret=os.environ.get('REDDIT_CLIENT_SECRET', ''),
                user_agent=os.environ.get('REDDIT_USER_AGENT', 'AcademicResearch:NootropicsStudy:v1.0')
            )
            service = RedditService(creds_obj)
            st.session_state.active_data_source = 'praw'
            final_user_agent = creds_obj.user_agent

            # --- Pre-flight connection test ---
            # Catches unreachable Reddit, network issues, and IP blocks
            # before committing to a multi-minute collection job.
            with st.spinner("Testing connection to Reddit..."):
                try:
                    if hasattr(service, 'verify_connection'):
                        ok = service.verify_connection()
                    elif hasattr(service, 'verify_credentials'):
                        ok = service.verify_credentials()
                    else:
                        ok = True  # no check available — proceed optimistically

                    if not ok:
                        st.error(
                            "❌ Could not reach Reddit. Check your internet connection "
                            "and try again. If the problem persists, Reddit may be "
                            "temporarily blocking requests — wait a few minutes."
                        )
                        return
                except Exception as _conn_err:
                    st.error(f"❌ Connection test failed: {_conn_err}")
                    return

            params = CollectionParams(
                subreddits=subreddits,
                method=collection_method,
                time_filter=time_filter,
                limit=limit,
                search_query=search_query,
                collect_comments=collect_comments,
                comment_limit=comment_limit,
                include_nsfw=include_nsfw,
                include_removed=include_removed,
                include_media_only=include_media_only,
                flag_non_english=flag_non_english,
                max_text_length=max_text_length,
                min_word_count_val=min_word_count_val,
                user_agent=final_user_agent,
                session_label=st.session_state.get('session_label'),
            )

            # Bind session context — required by both RedditService and RedditJSONService
            # for incremental DB saves and session-scoped codebook prune.
            params.session_id = st.session_state.session_id
            params.collection_started = datetime.utcnow().isoformat()

            # Start the background job
            job_id = JobManager.start_job(service, params)
            st.session_state.scraping_job_id = job_id
            st.rerun()

        except Exception as e:
            st.error(f"Failed to start collection job: {e}")
            return

    # --- POLLING LOGIC ---
    if st.session_state.scraping_job_id:
        from services.job_manager import JobManager
        from core.schemas import JobStatus
        import time

        job_id = st.session_state.scraping_job_id
        job_state = JobManager.get_job(job_id)

        if not job_state:
            st.warning("Job state not found. It may have been cleared.")
            st.session_state.scraping_job_id = None
            st.rerun()

        st.info(f"Job **{job_id[:8]}** is currently **{job_state.status.value}**.")

        if job_state.status in [JobStatus.PENDING, JobStatus.RUNNING]:
            if job_state.progress:
                st.progress(job_state.progress.progress_percentage)
                st.text(job_state.progress.status_message)
                
                # Time estimate — 20s per subreddit at 10 req/min (3 pages × 6s + overhead)
                pct = job_state.progress.progress_percentage
                if 0 < pct < 1.0:
                    # Estimate based on known subreddit count and current progress
                    _est_secs_per_sub = 20
                    _remaining_frac = 1.0 - pct
                    _total_subs = len(subreddits) if subreddits else 5
                    _remaining_subs = max(1, round(_remaining_frac * _total_subs))
                    _est_remaining = _remaining_subs * _est_secs_per_sub
                    if _est_remaining > 60:
                        st.caption(f"⏱️ Estimated ~{_est_remaining // 60} min {_est_remaining % 60}s remaining")
                    else:
                        st.caption(f"⏱️ Estimated ~{_est_remaining}s remaining")
                
                if job_state.progress.rate_stats:
                    rate = job_state.progress.rate_stats
                    col1, col2, col3 = st.columns(3)
                    
                    budget_label = "API Budget"
                    
                    col1.metric(budget_label, f"{rate.get('requests_this_window', 0)}/{rate.get('requests_per_minute_limit', 60)}")
                    col2.metric("Window Resets", f"{rate.get('window_remaining_seconds', 0):.0f}s")
            else:
                st.progress(0.0)
                st.text("Initializing data collection stream...")
            
            cancel_col, _ = st.columns([1, 5])
            with cancel_col:
                if st.button("Cancel Job", type="secondary"):
                    JobManager.cancel_job(job_id)
                    st.toast("Cancellation requested...")
                    st.rerun()

            # Poll every second
            time.sleep(1)
            st.rerun()

        elif job_state.status == JobStatus.FAILED:
            st.error(f"Data collection failed: {job_state.error}")
            if st.button("Clear Job State"):
                JobManager.clear_job(job_id)
                st.session_state.scraping_job_id = None
                st.rerun()

        elif job_state.status == JobStatus.CANCELLED:
            st.warning("Data collection was cancelled by the user.")
            if st.button("Clear Job State"):
                JobManager.clear_job(job_id)
                st.session_state.scraping_job_id = None
                st.rerun()

        elif job_state.status == JobStatus.COMPLETED:
            final_result = job_state.result
            if not final_result:
                st.error("Job completed but yielded no results object.")
                if st.button("Clear Job State"):
                    JobManager.clear_job(job_id)
                    st.session_state.scraping_job_id = None
                    st.rerun()
                return

            collection_hash = final_result.collection_hash
            # collected_posts is always [] — buffer cleared by incremental saves in both
            # PRAW and JSON services. Use stats.total_collected for count; reload from DB
            # for actual data (done below via load_collected_data).
            rate_limit_events = final_result.rate_limit_events
            collection_params = {
                'collection_completed': final_result.collection_completed,
                'collection_hash': collection_hash,
                'data_source': st.session_state.get('active_data_source', 'praw'),
                'collected_at': datetime.utcnow().isoformat(),
            }
            # Save data (already done incrementally in background thread)
            saved_count = final_result.stats.total_collected
            
            # Log replicability
            save_replicability_log(
                collection_hash=collection_hash,
                session_id=st.session_state.session_id,
                parameters={
                    'subreddits': subreddits,
                    'method': collection_method,
                    'time_filter': time_filter,
                    'limit': limit,
                    'search_query': search_query,
                    'collect_comments': collect_comments,
                    'comment_limit': comment_limit,
                    'include_nsfw': include_nsfw,
                    'include_removed': include_removed,
                    'include_media_only': include_media_only,
                    'flag_non_english': flag_non_english,
                    'max_text_length': max_text_length,
                    'data_source': st.session_state.get('active_data_source', 'praw')
                },
                statistics=final_result.stats.model_dump(),
                rate_limit_events=rate_limit_events if rate_limit_events else None,
                validation_results=final_result.validation,
                notes=f"Collection completed at {datetime.utcnow().isoformat()}"
            )

            # Log action
            log_action(
                action='data_collection',
                session_id=st.session_state.session_id,
                details={'collection_hash': collection_hash, 'saved_count': saved_count, 'data_source': st.session_state.get('active_data_source', 'praw')}
            )

            # Persist session label to ScrapeRun.extra_metadata so it appears
            # in the Session Management tab and dashboard filter.
            _label = st.session_state.get('session_label', '')
            if _label:
                update_session_metadata(
                    session_id=st.session_state.session_id,
                    label=_label,
                )

            from utils.db_helpers import load_collected_data
            st.session_state.collected_data = load_collected_data(session_id=None, limit=10000)
            st.session_state.active_hash = collection_hash
            
            if 'collection_runs' not in st.session_state:
                st.session_state.collection_runs = []
            st.session_state.collection_runs.append(collection_params)

            active_src = st.session_state.get('active_data_source', 'praw')
            src_label = "via Reddit API (PRAW)"
            if saved_count == 0:
                st.warning(
                    f"⚠️ **Collection completed but saved 0 items** {src_label}.\n\n"
                    "This usually means Reddit rate-limited every request. "
                    "Running multiple collections in quick succession exhausts your API budget.\n\n"
                    "**Try:** Wait a minute, then click **Start New Collection** below. "
                    "If the problem persists, check the Collection Statistics expander "
                    "for Rate Limit Events."
                )
            else:
                st.success(f"Successfully collected and saved {saved_count} items {src_label}.")
            st.info(
                "**Next step →** Go to **🤖 Automated Qualitative Coding** in the "
                "sidebar to assign PPM codes to your collected posts."
            )

            with st.expander("Collection Statistics & Replicability Info"):
                col_a, col_b, col_c = st.columns(3)

                # Derived from DB reload — reflects all sessions, not this run only.
                # This-run total is saved_count above.
                db_posts = sum(1 for p in st.session_state.collected_data if p.get('type') == 'submission')
                db_comments = sum(1 for p in st.session_state.collected_data if p.get('type') == 'comment')

                with col_a:
                    st.metric("This Run", saved_count)
                    st.metric("Posts (DB total)", db_posts)
                    st.metric("Comments (DB total)", db_comments)

                with col_b:
                    st.metric("Skipped NSFW", final_result.stats.skipped_nsfw)
                    st.metric("Skipped Removed", final_result.stats.skipped_removed)
                    st.metric("Skipped Media-Only", final_result.stats.skipped_media_only)

                with col_c:
                    st.metric("Flagged Non-English", final_result.stats.flagged_non_english)
                    st.metric("Rate Limit Events", len(rate_limit_events))
                    st.metric("Collection Hash", collection_hash[:8] + "...")

                # --- Per-subreddit temporal breakdown ---
                this_run = [
                    item for item in st.session_state.collected_data
                    if item.get('session_id') == st.session_state.session_id
                ]
                if this_run:
                    st.divider()
                    st.write("**Per-Subreddit Breakdown (this session)**")

                    run_df = pd.DataFrame(this_run)
                    if 'subreddit' in run_df.columns and 'created_utc' in run_df.columns:
                        valid = run_df[run_df['created_utc'].notna() & (run_df['created_utc'] > 0)].copy()
                        if not valid.empty:
                            valid['year'] = pd.to_datetime(
                                valid['created_utc'], unit='s', errors='coerce'
                            ).dt.year

                            sub_stats = valid.groupby('subreddit').agg(
                                posts=('subreddit', 'count'),
                                earliest=('year', 'min'),
                                latest=('year', 'max'),
                                avg_score=('score', 'mean'),
                            ).reset_index()
                            sub_stats['avg_score'] = sub_stats['avg_score'].round(0).astype(int)
                            sub_stats.columns = ['Subreddit', 'Posts', 'Earliest Pst (Yr)', 'Latest Pst (Yr)', 'Avg Score']

                            st.dataframe(sub_stats, use_container_width=True, hide_index=True)

                            # Year × subreddit pivot for temporal gap detection
                            pivot = valid.pivot_table(
                                index='subreddit', columns='year',
                                values='id', aggfunc='count', fill_value=0,
                            )
                            pivot.columns = [str(int(y)) for y in pivot.columns]
                            st.write("**Posts by Year × Subreddit**")
                            st.dataframe(pivot, use_container_width=True)

            if st.button("Start New Collection"):
                JobManager.clear_job(job_id)
                st.session_state.scraping_job_id = None
                st.rerun()

    st.divider()

    if st.session_state.collected_data:
        st.subheader("Collected Data Preview")

        df = pd.DataFrame(st.session_state.collected_data)

        col_stats1, col_stats2, col_stats3 = st.columns(3)
        with col_stats1:
            st.metric("Total Items", len(df))
        with col_stats2:
            nsfw_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('nsfw', False))
            st.metric("NSFW Items", nsfw_count)
        with col_stats3:
            removed_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('content_status') in ['removed', 'author_deleted'])
            st.metric("Removed/Deleted", removed_count)

        filter_col1, filter_col2, filter_col3 = st.columns(3)

        with filter_col1:
            filter_type = st.selectbox("Filter by Type", ["All", "Posts Only", "Comments Only"])

        with filter_col2:
            filter_nsfw = st.selectbox("Filter NSFW", ["All", "NSFW Only", "Non-NSFW Only"])

        with filter_col3:
            filter_status = st.selectbox("Filter Status", ["All", "Available Only", "Removed/Deleted Only"])

        df_display = df.copy()

        if filter_type == "Posts Only":
            df_display = df_display[df_display['type'] == 'submission']
        elif filter_type == "Comments Only":
            df_display = df_display[df_display['type'] == 'comment']

        df_display['_nsfw'] = df_display.apply(
            lambda r: r.get('metadata', {}).get('nsfw', False) if isinstance(r.get('metadata'), dict) else False, axis=1
        )
        df_display['_status'] = df_display.apply(
            lambda r: r.get('metadata', {}).get('content_status', 'available') if isinstance(r.get('metadata'), dict) else 'available', axis=1
        )
        
        if filter_nsfw == "NSFW Only":
            df_display = df_display[df_display['_nsfw'] == True]
        elif filter_nsfw == "Non-NSFW Only":
            df_display = df_display[df_display['_nsfw'] == False]

        if filter_status == "Available Only":
            df_display = df_display[df_display['_status'] == 'available']

        display_cols = ['data_source', 'type', 'subreddit', 'title', 'author', 'score', 'text']
        available_cols = [c for c in display_cols if c in df_display.columns]

        st.dataframe(
            df_display[available_cols].head(50),
            use_container_width=True
        )

        with st.expander("View Full Data (JSON)"):
            st.json(st.session_state.collected_data[:10])

        with st.expander("View Collection Audit Trail"):
            if 'collection_runs' in st.session_state and st.session_state.collection_runs:
                for i, run in enumerate(st.session_state.collection_runs):
                    st.markdown(f"**Collection Run {i+1}:** {run.get('collection_started', 'N/A')}")
                    st.json(run)
            else:
                st.info("No collection runs recorded in this session.")
