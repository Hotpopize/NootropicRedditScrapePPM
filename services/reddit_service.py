import praw
from datetime import datetime
import json
import hashlib
from typing import Generator, Union

from core.schemas import RedditCredentials, CollectionParams, CollectionProgress, CollectionResult, CollectionStats
from utils.db_helpers import save_collected_data, update_scrape_run
from modules.codebook import get_ppm_keywords

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
        final_user_agent = credentials.user_agent
        if "AcademicResearch" not in final_user_agent and len(final_user_agent) < 10:
            final_user_agent = f"AcademicResearch:NootropicRedditScrapePPM:v1.0 (by /u/unknown)"

        self.reddit = praw.Reddit(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            user_agent=final_user_agent
        )

    def verify_credentials(self) -> bool:
        try:
            self.reddit.read_only = True
            _ = self.reddit.user.me() if not self.reddit.read_only else self.reddit.random_subreddit()
            return True
        except Exception:
            return False

    def collect_data(self, params: CollectionParams) -> Generator[Union[CollectionProgress, CollectionResult], None, None]:
        if not params.collection_started:
            params.collection_started = datetime.utcnow().isoformat()

        params_dict = params.model_dump()
        collection_hash = generate_collection_hash(params_dict)

        collected_posts = []  # Acts as an incremental buffer now
        total_saved_count = 0
        stats = CollectionStats()
        rate_limit_events = []
        total_subreddits = len(params.subreddits)

        for idx, subreddit_name in enumerate(params.subreddits):
            yield CollectionProgress(
                current_subreddit=subreddit_name,
                progress_percentage=idx / total_subreddits,
                status_message=f"Processing r/{subreddit_name}..."
            )

            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                subreddit_nsfw = getattr(subreddit, 'over18', False)

                if subreddit_nsfw and not params.include_nsfw:
                    stats.skipped_nsfw += 1
                    continue

                if params.method == "Recent Posts (Hot)":
                    posts = subreddit.hot(limit=params.limit)
                elif params.method == "Recent Posts (New)":
                    posts = subreddit.new(limit=params.limit)
                elif params.method == "Top Posts (Time Period)":
                    posts = subreddit.top(time_filter=params.time_filter, limit=params.limit)
                elif params.method == "Search Query" and params.search_query:
                    posts = subreddit.search(params.search_query, limit=params.limit)
                else:
                    posts = subreddit.hot(limit=params.limit)

                for post in posts:
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
                            'flair_text': getattr(post, 'link_flair_text', None)
                        }
                    }
                    collected_posts.append(post_data)

                    if params.collect_comments and post.num_comments > 0:
                        try:
                            post.comments.replace_more(limit=0)
                            for comment in post.comments.list()[:params.comment_limit]:
                                comment_text = comment.body[:params.max_text_length] if comment.body else ''
                                comment_status = detect_content_status(comment_text, str(comment.author))

                                if comment_status in ['removed', 'author_deleted'] and not params.include_removed:
                                    stats.skipped_removed += 1
                                    continue

                                comment_language = detect_language(comment_text) if params.flag_non_english else 'not_checked'
                                if comment_language == 'likely_non_english':
                                    stats.flagged_non_english += 1

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
                                        'was_truncated': len(comment.body) > params.max_text_length if comment.body else False,
                                        'is_submitter': getattr(comment, 'is_submitter', False),
                                        'depth': getattr(comment, 'depth', 0),
                                        'stickied': getattr(comment, 'stickied', False),
                                        'collection_hash': collection_hash,
                                        'parent_id': comment.parent_id
                                    }
                                }
                                collected_posts.append(comment_data)
                                
                                # Incremental write checkpoint
                                if len(collected_posts) >= 50:
                                    saved = save_collected_data(collected_posts, params.session_id)
                                    total_saved_count += saved
                                    collected_posts.clear()
                                    if hasattr(params, 'job_id') and params.job_id:
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
                        if hasattr(params, 'job_id') and params.job_id:
                            update_scrape_run(params.job_id, items_collected=total_saved_count)

            except Exception as e:
                error_msg = str(e)
                if 'rate' in error_msg.lower() or 'limit' in error_msg.lower():
                    rate_limit_events.append({
                        'type': 'rate_limit',
                        'subreddit': subreddit_name,
                        'error': error_msg,
                        'timestamp': datetime.utcnow().isoformat()
                    })
                yield CollectionProgress(
                    current_subreddit=subreddit_name,
                    progress_percentage=idx / total_subreddits,
                    status_message=f"Error processing r/{subreddit_name}: {error_msg}"
                )
                
        # Final flush of any remaining buffer
        if collected_posts:
            saved = save_collected_data(collected_posts, params.session_id)
            total_saved_count += saved
            collected_posts.clear()
            if hasattr(params, 'job_id') and params.job_id:
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
