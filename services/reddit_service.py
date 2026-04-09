"""
services/reddit_service.py
==========================
Authenticated Reddit data collection via PRAW.

Compliance: Reddit Research Data Addendum (executed 2026-04-01)
---------------------------------------------------------------------------
This module implements authenticated-only access (§1) and 
PII pseudonymisation at ingestion time (§2.b).
See COMPLIANCE.md in the repository root for the full mapping.

Collects posts and comments using Reddit's official API. Implements the
collect_data() generator interface so JobManager can run it in a background
thread.

Exports
-------
- RedditService: Main collection service class.
- RateLimiter: Token bucket rate limiter.
- reddit_retry: Tenacity decorator for PRAW network errors.
- detect_content_status: Helper for identifying removed/deleted content.
- detect_language: Helper for identifying non-English text.
- get_ppm_tags: Helper for matching text against the PPM codebook.
- generate_collection_hash: Utility for hashing collection parameters.
"""

import praw
import prawcore
from datetime import datetime
import json
import hashlib
import time
import logging
from typing import Generator, Union
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from core.schemas import RedditCredentials, CollectionParams, CollectionProgress, CollectionResult, CollectionStats, RateLimitConfig, CollectedItem
from utils.db_helpers import save_collected_data, update_scrape_run
from modules.codebook import get_ppm_keywords

logger = logging.getLogger(__name__)

# =============================================================================
# RATE LIMITER CLASS
# =============================================================================

class RateLimiter:
    """Token bucket rate limiter for Reddit API compliance."""
    
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0.0
        self.request_count = 0
        self.window_start = time.time()
    
    def wait(self):
        """Block until next request is allowed."""
        now = time.time()
        
        # Reset window every minute
        if now - self.window_start >= 60:
            self.request_count = 0
            self.window_start = now
        
        # Check if we've hit the limit this window
        if self.request_count >= self.requests_per_minute:
            sleep_time = 60 - (now - self.window_start)
            if sleep_time > 0:
                logger.info("Rate limit: sleeping %.1fs", sleep_time)
                time.sleep(sleep_time)
            self.request_count = 0
            self.window_start = time.time()
        
        # Enforce minimum interval between requests
        elapsed = now - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def get_stats(self) -> dict:
        return {
            "requests_this_window": self.request_count,
            "window_remaining_seconds": 60 - (time.time() - self.window_start),
            "requests_per_minute_limit": self.requests_per_minute
        }


# =============================================================================
# RETRY DECORATOR
# =============================================================================

def reddit_retry(func):
    """Decorator for Reddit API calls with exponential backoff."""
    config = RateLimitConfig()
    return retry(
        retry=retry_if_exception_type((
            prawcore.exceptions.TooManyRequests,
            prawcore.exceptions.ServerError,
            prawcore.exceptions.ResponseException,
        )),
        stop=stop_after_attempt(config.max_retries),
        wait=wait_exponential(
            multiplier=config.backoff_base,
            min=4,
            max=config.backoff_max
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )(func)


def get_ppm_tags(text):
    if not text:
        return []

    text_lower = text.lower()
    tags = set()
    ppm_keywords = get_ppm_keywords()

    for category, keywords in ppm_keywords.items():
        for keyword in keywords:
            # We enforce spaces around the keyword but doing so stringently might be bad,
            # so we just use "in text_lower" as it was before.
            if keyword in text_lower:
                tags.add(category)
                break

    return list(tags)

def detect_content_status(text, author):
    if text in ['[removed]', '[deleted]']:
        return 'removed'
    if author in ['[deleted]', None]:
        return 'author_deleted'
    if not text or text.strip() == '':
        return 'empty'
    return 'available'

def detect_language(text):
    if not text:
        return 'unknown'

    non_ascii = sum(1 for char in text if ord(char) > 127)
    total_chars = len(text) if text else 1
    non_ascii_ratio = non_ascii / total_chars

    if non_ascii_ratio > 0.3:
        return 'likely_non_english'
    return 'english'

def detect_content_type(post):
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

def generate_collection_hash(params_dict):
    param_str = json.dumps(params_dict, sort_keys=True)
    return hashlib.sha256(param_str.encode()).hexdigest()[:16]

class RedditService:
    def __init__(self, credentials: RedditCredentials):
        # Store credentials for use inside threads instead of instantiating praw.Reddit here
        self.credentials = credentials
        config = RateLimitConfig()
        self.rate_limiter = RateLimiter(requests_per_minute=config.requests_per_minute)

    def verify_credentials(self) -> bool:
        try:
            final_user_agent = self.credentials.user_agent
            if "AcademicResearch" not in final_user_agent and len(final_user_agent) < 10:
                final_user_agent = "AcademicResearch:NootropicRedditScrapePPM:v1.0 (by /u/unknown)"

            # Local PRAW instance just for verification
            reddit = praw.Reddit(
                client_id=self.credentials.client_id,
                client_secret=self.credentials.client_secret,
                user_agent=final_user_agent
            )
            reddit.read_only = True
            _ = reddit.user.me() if not reddit.read_only else reddit.random_subreddit()
            return True
        except prawcore.exceptions.OAuthException as e:
            logger.error("Reddit authentication failed (OAuthException): %s", e)
            return False
        except prawcore.exceptions.ResponseException as e:
            logger.error("Reddit authentication failed (ResponseException): %s", e)
            return False
        except Exception as e:
            logger.error("Reddit authentication encountered an unexpected error: %s", e)
            return False

    def collect_data(self, params: CollectionParams) -> Generator[Union[CollectionProgress, CollectionResult], None, None]:
        # CREATE REDDIT INSTANCE INSIDE WORKER THREAD FOR THREAD ISOLATION
        final_user_agent = self.credentials.user_agent
        if "AcademicResearch" not in final_user_agent and len(final_user_agent) < 10:
            final_user_agent = "AcademicResearch:NootropicRedditScrapePPM:v1.0 (by /u/unknown)"
        
        reddit = praw.Reddit(
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
            user_agent=final_user_agent
        )

        @reddit_retry
        def _fetch_comments_with_retry(post, limit: int):
            """Fetch comments with rate limiting."""
            self.rate_limiter.wait()
            post.comments.replace_more(limit=0)
            return post.comments.list()[:limit]

        @reddit_retry
        def _get_subreddit(name: str):
            """Fetch subreddit with retry protection."""
            self.rate_limiter.wait()
            sub = reddit.subreddit(name)  # USE LOCAL REDDIT INSTANCE
            _ = sub.id  # Force API call within retry scope
            return sub

        @reddit_retry
        def _fetch_posts(subreddit, method: str, limit: int, time_filter: str = "week", query: str = None) -> list:
            """Fetch posts with rate limiting and retry."""
            self.rate_limiter.wait()
            
            if method == "hot":
                return list(subreddit.hot(limit=limit))
            elif method == "new":
                return list(subreddit.new(limit=limit))
            elif method == "top":
                return list(subreddit.top(time_filter=time_filter, limit=limit))
            elif method == "search":
                if query and query.strip():
                    return list(subreddit.search(query, limit=limit))
                else:
                    logger.warning("Search filter requested but empty query provided for r/%s — falling back to 'hot'.", subreddit.display_name)
                    return list(subreddit.hot(limit=limit))
            return list(subreddit.hot(limit=limit))

        if not params.collection_started:
            params.collection_started = datetime.utcnow().isoformat()

        params_dict = params.model_dump()
        collection_hash = generate_collection_hash(params_dict)

        collected_posts = []  # Acts as an incremental buffer now
        total_saved_count = 0
        stats = CollectionStats()
        rate_limit_events = []
        total_subreddits = len(params.subreddits)
        safe_total_subreddits = max(total_subreddits, 1)

        for idx, subreddit_name in enumerate(params.subreddits):
            yield CollectionProgress(
                current_subreddit=subreddit_name,
                progress_percentage=idx / safe_total_subreddits,
                status_message=f"Processing r/{subreddit_name}...",
                rate_stats=self.rate_limiter.get_stats()
            )

            try:
                subreddit = _get_subreddit(subreddit_name)
                subreddit_nsfw = getattr(subreddit, 'over18', False)

                if subreddit_nsfw and not params.include_nsfw:
                    stats.skipped_nsfw += 1
                    continue

                method_map = {
                    "Recent Posts (Hot)": "hot",
                    "Recent Posts (New)": "new",
                    "Top Posts (Time Period)": "top",
                    "Search Query": "search"
                }
                method = method_map.get(params.method, "hot")

                posts = _fetch_posts(
                    subreddit,
                    method=method,
                    limit=params.limit,
                    time_filter=params.time_filter,
                    query=params.search_query
                )

                for post in posts:
                    if getattr(params, 'date_after', None) and post.created_utc < params.date_after:
                        continue
                    if getattr(params, 'date_before', None) and post.created_utc > params.date_before:
                        continue

                    post_nsfw = getattr(post, 'over_18', False)
                    if post_nsfw and not params.include_nsfw:
                        stats.skipped_nsfw += 1
                        continue

                    post_text = post.selftext[:params.max_text_length] if post.selftext else ''
                    word_count = len(post_text.split())
                    if word_count < params.min_word_count_val:
                        continue

                    content_status = detect_content_status(post_text, str(post.author))

                    if content_status in ['removed', 'author_deleted'] and not params.include_removed:
                        stats.skipped_removed += 1
                        continue

                    content_type = detect_content_type(post)
                    if content_type in ['image', 'video', 'link'] and not post_text and not params.include_media_only:
                        stats.skipped_media_only += 1
                        continue

                    language = detect_language(post_text) if params.flag_non_english else 'not_checked'
                    if language == 'likely_non_english':
                        stats.flagged_non_english += 1

                    full_content_for_tagging = f"{post.title} {post_text}"
                    auto_tags = get_ppm_tags(full_content_for_tagging)

                    raw_author = str(post.author) if post.author else '[deleted]'
                    safe_author = hashlib.sha256(raw_author.encode()).hexdigest() if raw_author not in ['[deleted]', '[removed]'] else raw_author

                    post_data = {
                        'id': post.id,
                        'type': 'submission',
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'text': post_text,
                        'author': safe_author,
                        'score': post.score,
                        'created_utc': post.created_utc,
                        'num_comments': post.num_comments,
                        'url': post.url,
                        'permalink': f"https://reddit.com{post.permalink}",
                        'data_source': 'praw',
                        'collected_at': datetime.utcnow().isoformat(),
                        'metadata': {
                            'nsfw': post_nsfw,
                            'subreddit_nsfw': subreddit_nsfw,
                            'content_status': content_status,
                            'content_type': content_type,
                            'language_flag': language,
                            'text_length': len(post_text),
                            'word_count': word_count,
                            'auto_tags': auto_tags,
                            'was_truncated': len(post.selftext) > params.max_text_length if post.selftext else False,
                            'upvote_ratio': getattr(post, 'upvote_ratio', None),
                            'is_original_content': getattr(post, 'is_original_content', False),
                            'is_crosspostable': getattr(post, 'is_crosspostable', True),
                            'spoiler': getattr(post, 'spoiler', False),
                            'stickied': getattr(post, 'stickied', False),
                            'locked': getattr(post, 'locked', False),
                            'collection_hash': collection_hash,
                            'data_source': 'praw',
                            'flair_text': getattr(post, 'link_flair_text', None)
                        }
                    }
                    CollectedItem.model_validate(post_data) # Enforce schema & PII validation
                    collected_posts.append(post_data)

                    if params.collect_comments and post.num_comments > 0:
                        try:
                            comments_list = self._fetch_comments_with_retry(post, params.comment_limit)
                            for comment in comments_list:
                                comment_text = comment.body[:params.max_text_length] if comment.body else ''
                                comment_status = detect_content_status(comment_text, str(comment.author))

                                if comment_status in ['removed', 'author_deleted'] and not params.include_removed:
                                    stats.skipped_removed += 1
                                    continue

                                comment_language = detect_language(comment_text) if params.flag_non_english else 'not_checked'
                                if comment_language == 'likely_non_english':
                                    stats.flagged_non_english += 1

                                raw_comment_author = str(comment.author) if comment.author else '[deleted]'
                                safe_comment_author = hashlib.sha256(raw_comment_author.encode()).hexdigest() if raw_comment_author not in ['[deleted]', '[removed]'] else raw_comment_author

                                comment_data = {
                                    'id': comment.id,
                                    'type': 'comment',
                                    'subreddit': subreddit_name,
                                    'post_id': post.id,
                                    'title': f"Comment on: {post.title[:100]}",
                                    'text': comment_text,
                                    'author': safe_comment_author,
                                    'score': comment.score,
                                    'created_utc': comment.created_utc,
                                    'permalink': f"https://reddit.com{comment.permalink}",
                                    'url': None,
                                    'num_comments': None,
                                    'data_source': 'praw',
                                    'collected_at': datetime.utcnow().isoformat(),
                                    'metadata': {
                                        'nsfw': post_nsfw,
                                        'content_status': comment_status,
                                        'language_flag': comment_language,
                                        'text_length': len(comment_text),
                                        'was_truncated': len(comment.body) > params.max_text_length if comment.body else False,
                                        'is_submitter': getattr(comment, 'is_submitter', False),
                                        'depth': getattr(comment, 'depth', 0),
                                        'stickied': getattr(comment, 'stickied', False),
                                        'collection_hash': collection_hash,
                                        'data_source': 'praw',
                                        'parent_id': comment.parent_id
                                    }
                                }
                                CollectedItem.model_validate(comment_data) # Enforce schema & PII validation
                                collected_posts.append(comment_data)
                                
                                # Incremental write checkpoint
                                if len(collected_posts) >= 50:
                                    saved = save_collected_data(collected_posts, params.session_id)
                                    total_saved_count += saved
                                    collected_posts.clear()
                                    if params.job_id:
                                        update_scrape_run(params.job_id, items_collected=total_saved_count)

                        except Exception as comment_error:
                            rate_limit_events.append({
                                'type': 'comment_fetch_error',
                                'post_id': post.id,
                                'error': str(comment_error),
                                'timestamp': datetime.utcnow().isoformat()
                            })
                            
                    # Incremental write checkpoint for posts without comments or if buffer is full
                    if len(collected_posts) >= 50:
                        saved = save_collected_data(collected_posts, params.session_id)
                        total_saved_count += saved
                        collected_posts.clear()
                        if params.job_id:
                            update_scrape_run(params.job_id, items_collected=total_saved_count)

                    yield CollectionProgress(
                        current_subreddit=subreddit_name,
                        progress_percentage=idx / safe_total_subreddits,
                        status_message=f"Processing post from r/{subreddit_name}...",
                        rate_stats=self.rate_limiter.get_stats()
                    )

            except prawcore.exceptions.TooManyRequests as e:
                rate_limit_events.append({
                    'type': 'rate_limit_hard',
                    'subreddit': subreddit_name,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                })
                logger.warning("Hard rate limit on r/%s, waiting 60s", subreddit_name)
                time.sleep(60)
                continue
                
            except prawcore.exceptions.Forbidden as e:
                rate_limit_events.append({
                    'type': 'forbidden_error',
                    'subreddit': subreddit_name,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                })
                logger.info("r/%s is private/banned, skipping", subreddit_name)
                continue
                
            except prawcore.exceptions.NotFound as e:
                rate_limit_events.append({
                    'type': 'not_found_error',
                    'subreddit': subreddit_name,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                })
                logger.info("r/%s not found, skipping", subreddit_name)
                continue
                
            except Exception as e:
                logger.error("Unexpected error on r/%s: %s", subreddit_name, e)
                error_msg = str(e)
                yield CollectionProgress(
                    current_subreddit=subreddit_name,
                    progress_percentage=idx / safe_total_subreddits,
                    status_message=f"Error processing r/{subreddit_name}: {error_msg}",
                    rate_stats=self.rate_limiter.get_stats()
                )
                
        # Final flush of any remaining buffer
        if collected_posts:
            saved = save_collected_data(collected_posts, params.session_id)
            total_saved_count += saved
            collected_posts.clear()
            if params.job_id:
                update_scrape_run(params.job_id, items_collected=total_saved_count)

        stats.total_collected = total_saved_count
        # Note: In a true streaming model, we would stream these stats as well, but for now we'll sum from the DB or approximate.
        # Setting to 0 to avoid breaking schemas; to get accurate numbers, we'd need to query the DB after.
        stats.nsfw_collected = 0
        stats.removed_collected = 0
        stats.truncated_collected = 0
        stats.media_only_collected = 0
        stats.non_english_collected = stats.flagged_non_english

        nsfw_subreddits = list(set(p.get('subreddit') for p in collected_posts if p.get('metadata', {}).get('subreddit_nsfw', False)))
        stats.nsfw_subreddits = nsfw_subreddits
        stats.nsfw_subreddits_count = len(nsfw_subreddits)

        validation = {
            'nsfw_collected': stats.nsfw_collected,
            'removed_collected': stats.removed_collected,
            'truncated_collected': stats.truncated_collected,
            'media_only_collected': stats.media_only_collected,
            'non_english_collected': stats.flagged_non_english,
            'nsfw_subreddits': stats.nsfw_subreddits
        }

        yield CollectionProgress(
            current_subreddit="Done",
            progress_percentage=1.0,
            status_message="Collection finished."
        )

        yield CollectionResult(
            collection_hash=collection_hash,
            collected_posts=collected_posts,
            rate_limit_events=rate_limit_events,
            stats=stats,
            validation=validation,
            collection_started=params.collection_started,
            collection_completed=datetime.utcnow().isoformat()
        )
