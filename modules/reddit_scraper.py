import streamlit as st
import praw
import pandas as pd
from datetime import datetime, timedelta
import json
import os
from utils.db_helpers import save_collected_data, log_action
from tenacity import retry, stop_after_attempt, wait_exponential

def render():
    st.header("🌐 Reddit Data Collection")
    
    st.info("""
    **PRAW (Python Reddit API Wrapper)** enables systematic data collection from Reddit.
    
    To use this tool, you'll need Reddit API credentials:
    1. Go to https://www.reddit.com/prefs/apps
    2. Click "Create App" or "Create Another App"
    3. Select "script" as the app type
    4. Fill in the required fields
    5. Copy your client ID, client secret, and set a user agent
    """)
    
    with st.expander("🔑 Reddit API Configuration", expanded=False):
        client_id = st.text_input("Client ID", type="password", help="Found under the app name")
        client_secret = st.text_input("Client Secret", type="password", help="The secret key shown")
        user_agent = st.text_input("User Agent", value="AcademicResearch:NootropicsStudy:v1.0 (by /u/YourUsername)", help="Descriptive string identifying your app")
        
        if st.button("💾 Save API Credentials"):
            st.session_state.reddit_credentials = {
                'client_id': client_id,
                'client_secret': client_secret,
                'user_agent': user_agent
            }
            st.success("✅ Credentials saved for this session")
    
    st.divider()
    
    st.subheader("🎯 Data Collection Parameters")
    
    col1, col2 = st.columns(2)
    
    with col1:
        subreddits_input = st.text_area(
            "Target Subreddits (one per line)",
            value="Nootropics\nStackAdvice\nSupplements\nCognitive_Neuroscience",
            help="Enter subreddit names without r/ prefix"
        )
        subreddits = [s.strip() for s in subreddits_input.split('\n') if s.strip()]
        
        search_query = st.text_input(
            "Search Query (optional)",
            value="",
            help="Leave empty to collect recent posts, or enter keywords"
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
    
    if st.button("🚀 Start Data Collection", type="primary"):
        if 'reddit_credentials' not in st.session_state or not st.session_state.reddit_credentials.get('client_id'):
            st.error("⚠️ Please configure Reddit API credentials first")
            return
        
        try:
            creds = st.session_state.reddit_credentials
            reddit = praw.Reddit(
                client_id=creds['client_id'],
                client_secret=creds['client_secret'],
                user_agent=creds['user_agent']
            )
            
            st.info("🔄 Collecting data... This may take a few minutes.")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            collected_posts = []
            total_subreddits = len(subreddits)
            
            for idx, subreddit_name in enumerate(subreddits):
                try:
                    status_text.text(f"Processing r/{subreddit_name}...")
                    subreddit = reddit.subreddit(subreddit_name)
                    
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
                        post_data = {
                            'id': post.id,
                            'type': 'post',
                            'subreddit': subreddit_name,
                            'title': post.title,
                            'text': post.selftext,
                            'author': str(post.author),
                            'score': post.score,
                            'created_utc': post.created_utc,
                            'num_comments': post.num_comments,
                            'url': post.url,
                            'permalink': f"https://reddit.com{post.permalink}",
                            'collected_at': datetime.now().isoformat()
                        }
                        collected_posts.append(post_data)
                        
                        if collect_comments and post.num_comments > 0:
                            post.comments.replace_more(limit=0)
                            for comment in post.comments.list()[:comment_limit]:
                                comment_data = {
                                    'id': comment.id,
                                    'type': 'comment',
                                    'subreddit': subreddit_name,
                                    'post_id': post.id,
                                    'title': f"Comment on: {post.title}",
                                    'text': comment.body,
                                    'author': str(comment.author),
                                    'score': comment.score,
                                    'created_utc': comment.created_utc,
                                    'permalink': f"https://reddit.com{comment.permalink}",
                                    'collected_at': datetime.now().isoformat()
                                }
                                collected_posts.append(comment_data)
                    
                    progress_bar.progress((idx + 1) / total_subreddits)
                
                except Exception as e:
                    st.warning(f"⚠️ Error processing r/{subreddit_name}: {str(e)}")
            
            saved_count = save_collected_data(collected_posts, st.session_state.session_id)
            st.session_state.collected_data.extend(collected_posts)
            
            log_action(
                action='data_collection',
                session_id=st.session_state.session_id,
                details={
                    'subreddits': subreddits,
                    'method': collection_method,
                    'posts_collected': len(collected_posts),
                    'saved_to_db': saved_count
                }
            )
            
            status_text.empty()
            progress_bar.empty()
            
            st.success(f"✅ Successfully collected and saved {saved_count} items ({sum(1 for p in collected_posts if p['type'] == 'post')} posts, {sum(1 for p in collected_posts if p['type'] == 'comment')} comments)")
            
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.write("Please check your API credentials and try again.")
    
    st.divider()
    
    if st.session_state.collected_data:
        st.subheader("📋 Collected Data Preview")
        
        df = pd.DataFrame(st.session_state.collected_data)
        
        st.write(f"**Total Items:** {len(df)}")
        
        filter_type = st.selectbox("Filter by Type", ["All", "Posts Only", "Comments Only"])
        
        if filter_type == "Posts Only":
            df_display = df[df['type'] == 'post']
        elif filter_type == "Comments Only":
            df_display = df[df['type'] == 'comment']
        else:
            df_display = df
        
        st.dataframe(
            df_display[['type', 'subreddit', 'title', 'author', 'score', 'text']].head(50),
            use_container_width=True
        )
        
        with st.expander("📄 View Full Data (JSON)"):
            st.json(st.session_state.collected_data[:10])
