import requests
import time
import logging
from datetime import datetime
from typing import Generator, Union
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from core.schemas import CollectionParams, CollectionProgress, CollectionResult, CollectionStats
from utils.db_helpers import save_collected_data, update_scrape_run
from services.reddit_service import (
    RateLimiter,
    detect_content_status,
    detect_language,
    get_ppm_tags,
    generate_collection_hash
)

logger = logging.getLogger(__name__)

def json_retry(func):
    return retry(
        retry=retry_if_exception_type((
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        reraise=True
    )(func)


class RedditJSONService:
    def __init__(self, user_agent: str = None):
        """
        No credentials required.
        user_agent defaults to the same format as RedditService if not provided.
        """
        if not user_agent or len(user_agent) < 10:
            self.user_agent = "AcademicResearch:NootropicRedditScrapePPM:v1.0 (by /u/unknown)"
        else:
            self.user_agent = user_agent
        
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        
        # Default to 30 requests/minute for JSON endpoint (more conservative)
        self.rate_limiter = RateLimiter(requests_per_minute=30)
        
    @json_retry
    def _fetch_page(self, subreddit: str, sort: str, after: str | None, t: str = "all", query: str = None) -> dict:
        """
        Single paginated request. Calls rate_limiter.wait() BEFORE request.
        Raises on 429 after backoff exhausted.
        Returns raw parsed JSON.
        """
        self.rate_limiter.wait()
        
        base_url = f"https://www.reddit.com/r/{subreddit}"
        
        params = {"limit": 100}
        if after:
            params["after"] = after
            
        if sort == "search" and query:
            url = f"{base_url}/search.json"
            params["q"] = query
            params["sort"] = "relevance"
            params["restrict_sr"] = 1
        elif sort in ["top", "hot", "new"]:
            url = f"{base_url}/{sort}.json"
            if sort == "top":
                params["t"] = t
        else:
            # fallback
            url = f"{base_url}/hot.json"
            
        backoffs = [5, 10, 20]
        attempts = 0
        while True:
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 429:
                if attempts < len(backoffs):
                    sleep_time = backoffs[attempts]
                    logger.warning(f"JSON rate limit 429 hit. Sleeping {sleep_time} seconds.")
                    time.sleep(sleep_time)
                    attempts += 1
                    continue
                else:
                    response.raise_for_status() # Exhausted backoff
            
            response.raise_for_status()
            break
            
        try:
            response_json = response.json()
        except ValueError:
            return {}
            
        # Response Schema Validation
        if "data" not in response_json or "children" not in response_json["data"]:
            logger.warning(f"Unexpected JSON schema: {response_json}")
            return {}
            
        return response_json

    def fetch_posts(self, subreddit: str, sort: str = "top",
                    max_posts: int = 100, keyword: str = None,
                    date_after: float = 1577836800.0,
                    date_before: float = 1767225600.0) -> list[dict]:
        """
        Paginates until date boundary, max_posts, or cursor exhausted.
        Returns list of item dicts ready for save_collected_data().
        Logs skipped duplicates (already-stored reddit_ids) to stdout.
        """
        posts = []
        after_cursor = None
        
        while len(posts) < max_posts:
            page_data = self._fetch_page(subreddit, sort, after_cursor, query=keyword)
            if not page_data:
                break
                
            children = page_data.get("data", {}).get("children", [])
            if not children:
                break
                
            last_created_utc = None
            for child in children:
                data = child.get("data", {})
                
                # Check date boundaries
                created_utc = float(data.get("created_utc", 0.0))
                last_created_utc = created_utc
                
                if created_utc < date_after or created_utc > date_before:
                    continue
                
                # Convert bare JSON data to item dict
                item = {
                    'raw_data': data, # Stored temporarily for collect_data mappings
                    'id': data.get('id'),
                    'type': 'submission',
                    'subreddit': data.get('subreddit', subreddit),
                    'title': data.get('title', ''),
                    'text': data.get('selftext', ''),
                    'author': data.get('author') if data.get('author') else '[deleted]',
                    'score': data.get('score', 0),
                    'created_utc': created_utc,
                    'num_comments': data.get('num_comments', 0),
                    'url': data.get('url', ''),
                    'permalink': "https://reddit.com" + data.get('permalink', ''),
                    'post_id': None,
                    'data_source': 'json_endpoint',
                    'metadata': {
                        'data_source': 'json_endpoint'
                    }
                }
                posts.append(item)
                if len(posts) >= max_posts:
                    break
                    
            after_cursor = page_data.get("data", {}).get("after")
            
            # Terminator
            if not after_cursor:
                break
            if last_created_utc and last_created_utc < date_after:
                break

        return posts

    def verify_connection(self) -> bool:
        """
        Lightweight check: fetch 1 post from r/Nootropics.
        Returns True if successful, False otherwise.
        """
        try:
            self._fetch_page("Nootropics", sort="top", after=None)
            return True
        except Exception as e:
            logger.error(f"JSON verification failed: {e}")
            return False

    def collect_data(self, params: CollectionParams) -> Generator[Union[CollectionProgress, CollectionResult], None, None]:
        """
        Drop-in interface mirror of RedditService.collect_data().
        Yields CollectionProgress and CollectionResult.
        """
        if not params.collection_started:
            params.collection_started = datetime.utcnow().isoformat()

        session_id = getattr(params, 'session_id', None)
        job_id = getattr(params, 'job_id', None)
        params_dict = params.model_dump()
        collection_hash = generate_collection_hash(params_dict)

        collected_posts = []
        total_saved_count = 0
        stats = CollectionStats()
        rate_limit_events = []
        total_subreddits = len(params.subreddits)
        
        if params.collect_comments:
            logger.warning("collect_comments flag is True, but JSON endpoint cannot fetch comments incrementally here. Ignoring.")

        for idx, subreddit_name in enumerate(params.subreddits):
            yield CollectionProgress(
                current_subreddit=subreddit_name,
                progress_percentage=idx / total_subreddits,
                status_message=f"Processing r/{subreddit_name} (JSON)...",
                rate_stats=self.rate_limiter.get_stats()
            )

            try:
                method_map = {
                    "Recent Posts (Hot)": "hot",
                    "Recent Posts (New)": "new",
                    "Top Posts (Time Period)": "top",
                    "Search Query": "search"
                }
                sort_method = method_map.get(params.method, "hot")

                # Note: the JSON scraper respects date boundary filters if specified outside params
                # But to replicate RedditService, we will just use fetch_posts which defaults to 2020.
                fetched_items = self.fetch_posts(
                    subreddit=subreddit_name,
                    sort=sort_method,
                    max_posts=params.limit,
                    keyword=params.search_query
                )

                for item in fetched_items:
                    raw_data = item.pop('raw_data', {})
                    
                    subreddit_nsfw = raw_data.get('subreddit_type', '') == 'nsfw' or raw_data.get('over18', False)
                    post_nsfw = raw_data.get('over_18', False)
                    
                    if post_nsfw and not params.include_nsfw:
                        stats.skipped_nsfw += 1
                        continue

                    post_text = item['text'][:params.max_text_length] if item['text'] else ''
                    word_count = len(post_text.split())
                    if word_count < params.min_word_count_val:
                        continue

                    content_status = detect_content_status(post_text, item['author'])

                    if content_status in ['removed', 'author_deleted'] and not params.include_removed:
                        stats.skipped_removed += 1
                        continue

                    # Simulate detect_content_type
                    url = item['url']
                    selftext = item['text']
                    content_type = 'text'
                    if selftext and len(selftext) > 10:
                        content_type = 'text'
                    elif any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']) or 'reddit.com/gallery' in url or 'i.redd.it' in url:
                        content_type = 'image'
                    elif any(ext in url.lower() for ext in ['.mp4', '.webm', '.mov']) or 'v.redd.it' in url or 'youtube.com' in url or 'youtu.be' in url:
                        content_type = 'video'
                    elif url and url != item['permalink']:
                        content_type = 'link'

                    if content_type in ['image', 'video', 'link'] and not post_text and not params.include_media_only:
                        stats.skipped_media_only += 1
                        continue

                    language = detect_language(post_text) if params.flag_non_english else 'not_checked'
                    if language == 'likely_non_english':
                        stats.flagged_non_english += 1

                    full_content_for_tagging = f"{item['title']} {post_text}"
                    auto_tags = get_ppm_tags(full_content_for_tagging)
                    
                    # Update metadata with derived values mapping to the expected schema
                    item['metadata'].update({
                        'nsfw': post_nsfw,
                        'subreddit_nsfw': subreddit_nsfw,
                        'content_status': content_status,
                        'content_type': content_type,
                        'language_flag': language,
                        'text_length': len(post_text),
                        'word_count': word_count,
                        'auto_tags': auto_tags,
                        'was_truncated': len(item['text']) > params.max_text_length if item['text'] else False,
                        'upvote_ratio': raw_data.get('upvote_ratio', None),
                        'is_original_content': raw_data.get('is_original_content', False),
                        'is_crosspostable': raw_data.get('is_crosspostable', True),
                        'spoiler': raw_data.get('spoiler', False),
                        'stickied': raw_data.get('stickied', False),
                        'locked': raw_data.get('locked', False),
                        'collection_hash': collection_hash,
                        'flair_text': raw_data.get('link_flair_text', None)
                    })
                    item['collected_at'] = datetime.utcnow().isoformat()

                    collected_posts.append(item)
                    
                    if len(collected_posts) >= 50:
                        saved = save_collected_data(collected_posts, session_id)
                        total_saved_count += saved
                        collected_posts.clear()
                        if job_id:
                            update_scrape_run(job_id, items_collected=total_saved_count)

                    yield CollectionProgress(
                        current_subreddit=subreddit_name,
                        progress_percentage=idx / total_subreddits,
                        status_message=f"Processing post from r/{subreddit_name} (JSON)...",
                        rate_stats=self.rate_limiter.get_stats()
                    )

            except Exception as e:
                logger.error(f"Unexpected error on r/{subreddit_name}: {e}")
                rate_limit_events.append({
                    'type': 'json_fetch_error',
                    'subreddit': subreddit_name,
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                })
                yield CollectionProgress(
                    current_subreddit=subreddit_name,
                    progress_percentage=idx / total_subreddits,
                    status_message=f"Error processing r/{subreddit_name}: {str(e)}",
                    rate_stats=self.rate_limiter.get_stats()
                )
                
        # Final flush
        if collected_posts:
            saved = save_collected_data(collected_posts, session_id)
            total_saved_count += saved
            collected_posts.clear()
            if job_id:
                update_scrape_run(job_id, items_collected=total_saved_count)

        stats.total_collected = total_saved_count
        stats.nsfw_collected = 0
        stats.removed_collected = 0
        stats.truncated_collected = 0
        stats.media_only_collected = 0
        stats.non_english_collected = stats.flagged_non_english

        nsfw_subreddits = list({p.get('subreddit') for p in collected_posts if p.get('metadata', {}).get('subreddit_nsfw', False)})
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
