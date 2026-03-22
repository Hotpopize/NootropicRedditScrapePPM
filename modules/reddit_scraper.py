import streamlit as st
import pandas as pd
from datetime import datetime

from utils.db_helpers import save_collected_data, log_action, save_replicability_log, get_all_zotero_keywords

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

    creds = st.session_state.get('reddit_credentials', {})
    has_creds = bool(creds.get('client_id') and creds.get('client_secret'))
    pref = st.session_state.get('data_source_preference', 'Auto (recommended)')
    active_mode = 'praw' if (pref == 'PRAW' or (pref == 'Auto (recommended)' and has_creds)) else 'json'

    if active_mode == 'praw':
        st.success("🔑 **Collection Mode: Reddit API (PRAW)** — Authenticated access active. Configure credentials below.")
    else:
        st.warning("🌐 **Collection Mode: JSON Endpoint** — No credentials required. Using Reddit's public endpoints. ToS-compliant for non-commercial academic use.")

    with st.expander("Reddit API Configuration", expanded=False):
        client_id = st.text_input("Client ID", type="password", help="Found under the app name")
        client_secret = st.text_input("Client Secret", type="password", help="The secret key shown")
        user_agent = st.text_input("User Agent", value="AcademicResearch:NootropicsStudy:v1.0 (by /u/YourUsername)", help="Descriptive string identifying your app")

        if st.button("Save API Credentials"):
            st.session_state.reddit_credentials = {
                'client_id': client_id,
                'client_secret': client_secret,
                'user_agent': user_agent
            }
            st.success("Credentials saved for this session")

        if st.button("Verify Credentials"):
            if 'reddit_credentials' not in st.session_state:
                st.error("Save credentials first")
            else:
                try:
                    from core.schemas import RedditCredentials
                    from services.reddit_service import RedditService

                    creds_data = st.session_state.reddit_credentials
                    creds = RedditCredentials(
                        client_id=creds_data['client_id'],
                        client_secret=creds_data['client_secret'],
                        user_agent=creds_data['user_agent']
                    )
                    service = RedditService(creds)

                    if service.verify_credentials():
                        st.success("Credentials Verified! Connection successful.")
                    else:
                        st.error("Verification Failed: Could not connect to Reddit APIs. Check your client ID/secret.")
                except Exception as e:
                    st.error(f"Verification Error: {e}")

    st.divider()

    try:
        zotero_keywords = get_all_zotero_keywords()
    except Exception as e:
        st.warning(f"Could not load Zotero keywords (Database or connection issue): {e}")
        zotero_keywords = []
    if zotero_keywords:
        with st.expander("📚 Zotero-Informed Search Keywords", expanded=False):
            st.markdown("**Keywords from your Zotero literature are available to guide data collection:**")

            col_a, col_b = st.columns([3, 1])
            with col_a:
                selected_zotero_kw = st.multiselect(
                    "Select keywords from your literature",
                    options=zotero_keywords,
                    default=st.session_state.get('selected_zotero_keywords', [])[:5],
                    help="These keywords are extracted from your synced Zotero references"
                )
                st.session_state.selected_zotero_keywords = selected_zotero_kw

            with col_b:
                st.metric("Available Keywords", len(zotero_keywords))

            if selected_zotero_kw:
                suggested_query = ' OR '.join(selected_zotero_kw[:5])
                st.info(f"**Suggested search query:** `{suggested_query}`")

                if st.button("Use as Search Query"):
                    st.session_state.zotero_search_query = suggested_query

    st.divider()

    st.subheader("Data Collection Parameters")

    # --- THESIS CONFIGURATION TOGGLE ---
    use_thesis_config = st.checkbox(
        "🎓 Use Thesis Configuration (Methodology Standard)",
        value=False,
        help="Enabling this pre-fills target subreddits and recommended settings per the thesis methodology."
    )

    if use_thesis_config:
        st.info("✅ **Thesis Mode Active**: Targeting 5 Core Subreddits | Min Word Count: 20 | Auto-Tagging Enabled")
        subreddits_default = "\n".join(THESIS_SUBREDDITS)
        min_word_count_default = 20
    else:
        subreddits_default = "Nootropics\nStackAdvice\nSupplements\nCognitive_Neuroscience"
        min_word_count_default = 0

    col1, col2 = st.columns(2)

    with col1:
        subreddits_input = st.text_area(
            "Target Subreddits (one per line)",
            value=subreddits_default,
            help="Enter subreddit names without r/ prefix",
            height=150
        )
        subreddits = [s.strip() for s in subreddits_input.split('\n') if s.strip()]

        default_query = st.session_state.get('zotero_search_query', '')
        search_query = st.text_input(
            "Search Query (optional)",
            value=default_query if default_query else '(adderall OR ritalin OR caffeine OR modafinil) AND (lions mane OR ashwagandha OR bacopa OR noopept)',
            help="Leave empty to collect recent posts, or enter keywords. Use Zotero keywords above for literature-informed search."
        )
    with col2:
        collection_method = st.selectbox(
            "Collection Method",
            ["Recent Posts (Hot)", "Recent Posts (New)", "Top Posts (Time Period)", "Search Query"]
        )

        if collection_method == "Top Posts (Time Period)":
            time_filter = st.selectbox(
                "Time Period",
                ["day", "week", "month", "year", "all"]
            )
        else:
            time_filter = "all"

        limit = st.number_input(
            "Number of Posts per Subreddit",
            min_value=1,
            max_value=1000,
            value=100,
            help="Reddit API limits apply"
        )

        pref_src = st.session_state.get('data_source_preference', 'Auto (recommended)')
        has_creds = bool(st.session_state.get('reddit_credentials', {}).get('client_id'))
        use_json_mode = (pref_src == "JSON Endpoint" or (pref_src == "Auto (recommended)" and not has_creds))
        
        collect_comments = st.checkbox(
            "Collect Comments",
            value=True,
            disabled=use_json_mode,
            help="Comment collection not available in JSON mode." if use_json_mode else "Collect top comments per post."
        )

        if collect_comments:
            comment_limit = st.number_input(
                "Max Comments per Post",
                min_value=1,
                max_value=500,
                value=50
            )
        else:
            comment_limit = 0

    st.divider()

    st.subheader("Content Filtering & Edge Cases")

    col3, col4 = st.columns(2)

    with col3:
        include_nsfw = st.checkbox(
            "Include NSFW Content",
            value=False,
            help="Include posts/subreddits marked as Not Safe For Work (18+)"
        )

        include_removed = st.checkbox(
            "Include Removed/Deleted Content",
            value=True,
            help="Include posts where content is [removed] or [deleted] - useful for tracking deletions"
        )

        include_media_only = st.checkbox(
            "Include Media-Only Posts",
            value=True,
            help="Include image/video posts without text content"
        )

    with col4:
        flag_non_english = st.checkbox(
            "Flag Non-English Content",
            value=True,
            help="Automatically detect and flag likely non-English posts"
        )

        max_text_length = st.number_input(
            "Max Text Length (chars)",
            min_value=1000,
            max_value=100000,
            value=50000,
            help="Truncate very long posts to this length"
        )

        min_word_count_val = st.number_input(
            "Min Word Count (Posts)",
            min_value=0,
            max_value=1000,
            value=min_word_count_default,
            help="Filter out posts shorter than this (Thesis Standard: >20 words)"
        )

    st.divider()

    st.subheader("Data Source Selection")
    
    if 'data_source_preference' not in st.session_state:
        st.session_state.data_source_preference = "Auto (recommended)"

    data_source = st.radio(
        "Data Source",
        ["Auto (recommended)", "PRAW", "JSON Endpoint"],
        index=["Auto (recommended)", "PRAW", "JSON Endpoint"].index(
            st.session_state.data_source_preference
        ),
        horizontal=True,
        key="data_source_radio"
    )
    st.session_state.data_source_preference = data_source
    
    use_json = (
        st.session_state.data_source_preference == "JSON Endpoint" or
        (st.session_state.data_source_preference == "Auto (recommended)" and
         not st.session_state.get('reddit_credentials', {}).get('client_id'))
    )

    if use_json:
        st.warning(
            "**JSON Mode Active** — No API credentials detected. "
            "Data will be collected from Reddit's public JSON endpoints. "
            "This is ToS-compliant for non-commercial academic use. "
            "Collection method will be recorded in the audit trail.",
            icon="⚠️"
        )
        
    if 'scraping_job_id' not in st.session_state:
        st.session_state.scraping_job_id = None

    if st.button("Start Data Collection", type="primary", disabled=st.session_state.scraping_job_id is not None):
        try:
            from core.schemas import RedditCredentials, CollectionParams
            from services.job_manager import JobManager
            
            # Resolve which service to use
            creds = st.session_state.get('reddit_credentials', {})
            has_credentials = bool(creds.get('client_id') and creds.get('client_secret'))
            pref = st.session_state.data_source_preference
            
            use_praw = (
                pref == "PRAW" or
                (pref == "Auto (recommended)" and has_credentials)
            )
            
            if use_praw and not has_credentials:
                st.error("PRAW selected but no credentials configured.")
                return
            
            if use_praw:
                from services.reddit_service import RedditService
                creds_obj = RedditCredentials(
                    client_id=creds['client_id'],
                    client_secret=creds['client_secret'],
                    user_agent=creds.get('user_agent', 'AcademicResearch:NootropicsStudy:v1.0 (by /u/YourUsername)')
                )
                service = RedditService(creds_obj)
                st.session_state.active_data_source = 'praw'
                final_user_agent = creds_obj.user_agent
            else:
                from services.reddit_json_service import RedditJSONService
                user_agent_val = creds.get('user_agent', None)
                service = RedditJSONService(user_agent=user_agent_val)
                st.session_state.active_data_source = 'json_endpoint'
                final_user_agent = service.user_agent

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
                user_agent=final_user_agent,
                min_word_count_val=min_word_count_val
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
                
                if job_state.progress.rate_stats:
                    rate = job_state.progress.rate_stats
                    col1, col2, col3 = st.columns(3)
                    
                    pref_poll = st.session_state.get('data_source_preference', 'Auto (recommended)')
                    has_creds_poll = bool(st.session_state.get('reddit_credentials', {}).get('client_id'))
                    use_json_poll = (pref_poll == "JSON Endpoint" or (pref_poll == "Auto (recommended)" and not has_creds_poll))
                    budget_label = "API Budget" if not use_json_poll else "Request Budget"
                    
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

            from utils.db_helpers import load_collected_data
            st.session_state.collected_data = load_collected_data(session_id=None, limit=10000)
            st.session_state.active_hash = collection_hash
            
            if 'collection_runs' not in st.session_state:
                st.session_state.collection_runs = []
            st.session_state.collection_runs.append(collection_params)

            active_src = st.session_state.get('active_data_source', 'praw')
            src_label = "via Reddit API (PRAW)" if active_src == 'praw' else "via JSON Endpoint"
            st.success(f"Successfully collected and saved {saved_count} items {src_label}.")

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
