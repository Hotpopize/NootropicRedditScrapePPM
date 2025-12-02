import streamlit as st
import praw
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import hashlib
import re
from utils.db_helpers import save_collected_data, log_action, save_replicability_log, get_all_zotero_keywords
from tenacity import retry, stop_after_attempt, wait_exponential
from prawcore.exceptions import ResponseException

def detect_content_status(text, author):
    """Detect if content has been removed or deleted."""
    if text in ['[removed]', '[deleted]']:
        return 'removed'
    if author in ['[deleted]', None]:
        return 'author_deleted'
    if not text or text.strip() == '':
        return 'empty'
    return 'available'

def detect_language(text):
    """Basic language detection based on character analysis."""
    if not text:
        return 'unknown'
    
    non_ascii = sum(1 for char in text if ord(char) > 127)
    total_chars = len(text) if text else 1
    non_ascii_ratio = non_ascii / total_chars
    
    if non_ascii_ratio > 0.3:
        return 'likely_non_english'
    return 'english'

def detect_content_type(post):
    """Detect the type of content (text, link, image, video, etc.)."""
    url = getattr(post, 'url', '')
    selftext = getattr(post, 'selftext', '')
    
    if selftext and len(selftext) > 10:
        return 'text'
    elif any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
        return 'image'
    elif any(ext in url.lower() for ext in ['.mp4', '.webm', '.mov']):
        return 'video'
    elif 'reddit.com/gallery' in url or 'i.redd.it' in url:
        return 'image'
    elif 'v.redd.it' in url or 'youtube.com' in url or 'youtu.be' in url:
        return 'video'
    elif url and url != post.permalink:
        return 'link'
    else:
        return 'text'

def generate_collection_hash(params):
    """Generate a hash of collection parameters for replicability tracking."""
    param_str = json.dumps(params, sort_keys=True)
    return hashlib.sha256(param_str.encode()).hexdigest()[:16]

def render():
    st.header("Reddit Data Collection")
    
    st.info("""
    **PRAW (Python Reddit API Wrapper)** enables systematic data collection from Reddit.
    
    To use this tool, you'll need Reddit API credentials:
    1. Go to https://www.reddit.com/prefs/apps
    2. Click "Create App" or "Create Another App"
    3. Select "script" as the app type
    4. Fill in the required fields
    5. Copy your client ID, client secret, and set a user agent
    """)
    
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
    
    st.divider()
    
    zotero_keywords = get_all_zotero_keywords()
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
    
    col1, col2 = st.columns(2)
    
    with col1:
        subreddits_input = st.text_area(
            "Target Subreddits (one per line)",
            value="Nootropics\nStackAdvice\nSupplements\nCognitive_Neuroscience",
            help="Enter subreddit names without r/ prefix"
        )
        subreddits = [s.strip() for s in subreddits_input.split('\n') if s.strip()]
        
        default_query = st.session_state.get('zotero_search_query', '')
        search_query = st.text_input(
            "Search Query (optional)",
            value=default_query,
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
        
        collect_comments = st.checkbox("Collect Comments", value=True)
        
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
    
    st.divider()
    
    if st.button("Start Data Collection", type="primary"):
        if 'reddit_credentials' not in st.session_state or not st.session_state.reddit_credentials.get('client_id'):
            st.error("Please configure Reddit API credentials first")
            return
        
        try:
            creds = st.session_state.reddit_credentials
            reddit = praw.Reddit(
                client_id=creds['client_id'],
                client_secret=creds['client_secret'],
                user_agent=creds['user_agent']
            )
            
            collection_params = {
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
                'collection_started': datetime.utcnow().isoformat(),
                'user_agent': creds['user_agent']
            }
            
            collection_hash = generate_collection_hash(collection_params)
            
            st.info("Collecting data... This may take a few minutes.")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            collected_posts = []
            skipped_nsfw = 0
            skipped_removed = 0
            skipped_media_only = 0
            flagged_non_english = 0
            rate_limit_events = []
            total_subreddits = len(subreddits)
            
            for idx, subreddit_name in enumerate(subreddits):
                try:
                    status_text.text(f"Processing r/{subreddit_name}...")
                    subreddit = reddit.subreddit(subreddit_name)
                    
                    subreddit_nsfw = getattr(subreddit, 'over18', False)
                    
                    if subreddit_nsfw and not include_nsfw:
                        st.warning(f"Skipping r/{subreddit_name} (NSFW subreddit)")
                        skipped_nsfw += 1
                        continue
                    
                    if collection_method == "Recent Posts (Hot)":
                        posts = subreddit.hot(limit=limit)
                    elif collection_method == "Recent Posts (New)":
                        posts = subreddit.new(limit=limit)
                    elif collection_method == "Top Posts (Time Period)":
                        posts = subreddit.top(time_filter=time_filter, limit=limit)
                    elif collection_method == "Search Query" and search_query:
                        posts = subreddit.search(search_query, limit=limit)
                    else:
                        posts = subreddit.hot(limit=limit)
                    
                    for post in posts:
                        post_nsfw = getattr(post, 'over_18', False)
                        if post_nsfw and not include_nsfw:
                            skipped_nsfw += 1
                            continue
                        
                        post_text = post.selftext[:max_text_length] if post.selftext else ''
                        content_status = detect_content_status(post_text, str(post.author))
                        
                        if content_status in ['removed', 'author_deleted'] and not include_removed:
                            skipped_removed += 1
                            continue
                        
                        content_type = detect_content_type(post)
                        if content_type in ['image', 'video', 'link'] and not post_text and not include_media_only:
                            skipped_media_only += 1
                            continue
                        
                        language = detect_language(post_text) if flag_non_english else 'not_checked'
                        if language == 'likely_non_english':
                            flagged_non_english += 1
                        
                        post_data = {
                            'id': post.id,
                            'type': 'submission',
                            'subreddit': subreddit_name,
                            'title': post.title,
                            'text': post_text,
                            'author': str(post.author) if post.author else '[deleted]',
                            'score': post.score,
                            'created_utc': post.created_utc,
                            'num_comments': post.num_comments,
                            'url': post.url,
                            'permalink': f"https://reddit.com{post.permalink}",
                            'collected_at': datetime.utcnow().isoformat(),
                            'metadata': {
                                'nsfw': post_nsfw,
                                'subreddit_nsfw': subreddit_nsfw,
                                'content_status': content_status,
                                'content_type': content_type,
                                'language_flag': language,
                                'text_length': len(post_text),
                                'was_truncated': len(post.selftext) > max_text_length if post.selftext else False,
                                'upvote_ratio': getattr(post, 'upvote_ratio', None),
                                'is_original_content': getattr(post, 'is_original_content', False),
                                'is_crosspostable': getattr(post, 'is_crosspostable', True),
                                'spoiler': getattr(post, 'spoiler', False),
                                'stickied': getattr(post, 'stickied', False),
                                'locked': getattr(post, 'locked', False),
                                'collection_hash': collection_hash,
                                'flair_text': getattr(post, 'link_flair_text', None)
                            }
                        }
                        collected_posts.append(post_data)
                        
                        if collect_comments and post.num_comments > 0:
                            try:
                                post.comments.replace_more(limit=0)
                                for comment in post.comments.list()[:comment_limit]:
                                    comment_text = comment.body[:max_text_length] if comment.body else ''
                                    comment_status = detect_content_status(comment_text, str(comment.author))
                                    
                                    if comment_status in ['removed', 'author_deleted'] and not include_removed:
                                        skipped_removed += 1
                                        continue
                                    
                                    comment_language = detect_language(comment_text) if flag_non_english else 'not_checked'
                                    if comment_language == 'likely_non_english':
                                        flagged_non_english += 1
                                    
                                    comment_data = {
                                        'id': comment.id,
                                        'type': 'comment',
                                        'subreddit': subreddit_name,
                                        'post_id': post.id,
                                        'title': f"Comment on: {post.title[:100]}",
                                        'text': comment_text,
                                        'author': str(comment.author) if comment.author else '[deleted]',
                                        'score': comment.score,
                                        'created_utc': comment.created_utc,
                                        'permalink': f"https://reddit.com{comment.permalink}",
                                        'collected_at': datetime.utcnow().isoformat(),
                                        'metadata': {
                                            'nsfw': post_nsfw,
                                            'content_status': comment_status,
                                            'language_flag': comment_language,
                                            'text_length': len(comment_text),
                                            'was_truncated': len(comment.body) > max_text_length if comment.body else False,
                                            'is_submitter': getattr(comment, 'is_submitter', False),
                                            'depth': getattr(comment, 'depth', 0),
                                            'stickied': getattr(comment, 'stickied', False),
                                            'collection_hash': collection_hash,
                                            'parent_id': comment.parent_id
                                        }
                                    }
                                    collected_posts.append(comment_data)
                            except Exception as comment_error:
                                rate_limit_events.append({
                                    'type': 'comment_fetch_error',
                                    'post_id': post.id,
                                    'error': str(comment_error),
                                    'timestamp': datetime.utcnow().isoformat()
                                })
                    
                    progress_bar.progress((idx + 1) / total_subreddits)
                
                except Exception as e:
                    error_msg = str(e)
                    if 'rate' in error_msg.lower() or 'limit' in error_msg.lower():
                        rate_limit_events.append({
                            'type': 'rate_limit',
                            'subreddit': subreddit_name,
                            'error': error_msg,
                            'timestamp': datetime.utcnow().isoformat()
                        })
                    st.warning(f"Error processing r/{subreddit_name}: {error_msg}")
            
            nsfw_collected = sum(1 for p in collected_posts if p.get('metadata', {}).get('nsfw', False))
            removed_collected = sum(1 for p in collected_posts if p.get('metadata', {}).get('content_status') in ['removed', 'author_deleted'])
            truncated_collected = sum(1 for p in collected_posts if p.get('metadata', {}).get('was_truncated', False))
            media_only_collected = sum(1 for p in collected_posts if p.get('metadata', {}).get('content_type') in ['image', 'video', 'link'])
            
            nsfw_subreddits = list(set(p.get('subreddit') for p in collected_posts if p.get('metadata', {}).get('subreddit_nsfw', False)))
            
            collection_params['collection_completed'] = datetime.utcnow().isoformat()
            collection_params['collection_hash'] = collection_hash
            collection_params['rate_limit_events'] = rate_limit_events
            collection_params['stats'] = {
                'total_collected': len(collected_posts),
                'skipped_nsfw': skipped_nsfw,
                'skipped_removed': skipped_removed,
                'skipped_media_only': skipped_media_only,
                'flagged_non_english': flagged_non_english,
                'nsfw_collected': nsfw_collected,
                'removed_collected': removed_collected,
                'truncated_collected': truncated_collected,
                'media_only_collected': media_only_collected,
                'non_english_collected': flagged_non_english,
                'nsfw_subreddits': nsfw_subreddits,
                'nsfw_subreddits_count': len(nsfw_subreddits)
            }
            collection_params['validation'] = {
                'nsfw_collected': nsfw_collected,
                'removed_collected': removed_collected,
                'truncated_collected': truncated_collected,
                'media_only_collected': media_only_collected,
                'non_english_collected': flagged_non_english,
                'nsfw_subreddits': nsfw_subreddits
            }
            
            saved_count = save_collected_data(collected_posts, st.session_state.session_id)
            st.session_state.collected_data.extend(collected_posts)
            
            log_action(
                action='data_collection',
                session_id=st.session_state.session_id,
                details=collection_params
            )
            
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
                    'max_text_length': max_text_length
                },
                statistics=collection_params['stats'],
                rate_limit_events=rate_limit_events if rate_limit_events else None,
                validation_results=collection_params['validation'],
                notes=f"Collection completed at {datetime.utcnow().isoformat()}"
            )
            
            if 'collection_runs' not in st.session_state:
                st.session_state.collection_runs = []
            st.session_state.collection_runs.append(collection_params)
            
            status_text.empty()
            progress_bar.empty()
            
            posts_count = sum(1 for p in collected_posts if p['type'] == 'submission')
            comments_count = sum(1 for p in collected_posts if p['type'] == 'comment')
            
            st.success(f"Successfully collected and saved {saved_count} items ({posts_count} posts, {comments_count} comments)")
            
            with st.expander("Collection Statistics & Replicability Info"):
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    st.metric("Total Items", len(collected_posts))
                    st.metric("Posts", posts_count)
                    st.metric("Comments", comments_count)
                
                with col_b:
                    st.metric("Skipped NSFW", skipped_nsfw)
                    st.metric("Skipped Removed", skipped_removed)
                    st.metric("Skipped Media-Only", skipped_media_only)
                
                with col_c:
                    st.metric("Flagged Non-English", flagged_non_english)
                    st.metric("Rate Limit Events", len(rate_limit_events))
                    st.metric("Collection Hash", collection_hash[:8] + "...")
                
                st.markdown("---")
                st.markdown("**Replicability Parameters:**")
                st.code(json.dumps({
                    'collection_hash': collection_hash,
                    'subreddits': subreddits,
                    'method': collection_method,
                    'time_filter': time_filter,
                    'limit': limit,
                    'include_nsfw': include_nsfw,
                    'include_removed': include_removed,
                    'collection_time': collection_params['collection_started']
                }, indent=2))
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.write("Please check your API credentials and try again.")
    
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
        
        display_cols = ['type', 'subreddit', 'title', 'author', 'score', 'text']
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
