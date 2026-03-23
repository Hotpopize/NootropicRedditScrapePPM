"""
services/reddit_json_service.py
================================
Unauthenticated Reddit data collection via public JSON endpoints.

Drop-in alternative to RedditService (PRAW) for use when API credentials
are unavailable. Implements the same collect_data() generator interface
so JobManager and modules/reddit_scraper.py require no changes.

Endpoint format
---------------
  GET https://www.reddit.com/r/{subreddit}/{sort}.json?limit=100&t=all&after={cursor}
  GET https://www.reddit.com/r/{subreddit}/search.json?q={query}&restrict_sr=1

No authentication required. Publicly accessible. ToS-compliant for
non-commercial academic use.

Retry architecture
------------------
Two independent retry mechanisms handle different failure modes:

  @json_retry (tenacity):
    Handles transient network failures — ConnectionError, Timeout.
    These are worth retrying because they are typically transient.
    Configured from RateLimitConfig for consistency with reddit_retry.

  Non-retryable errors (SSLError, TooManyRedirects):
    These indicate structural problems that retrying cannot fix.
    SSLError = certificate invalid or TLS handshake failed.
    TooManyRedirects = redirect loop (e.g. to login page).
    Both are caught in _fetch_page and re-raised immediately.

  429 inner loop (in _fetch_page):
    Handles HTTP 429 (Too Many Requests) — a semantic API response, not
    a network exception. tenacity's retry_if_exception_type cannot catch
    this without raising first. The inner loop backs off and retries with
    rate_limiter.wait() called before each retry attempt.

Edge cases handled
------------------
  Null fields:      id/permalink/title/created_utc/selftext/score guarded
                    with `or` coalescion — None values never reach type ops.
  Stickied posts:   Tracked via seen_ids set — never collected twice.
  Pagination loops: Hard _MAX_PAGES cap prevents runaway pagination.
  Empty subreddit:  Children list empty → loop breaks cleanly.
  Zero limit:       params.limit=0 → max(params.limit, 1) guard.
  Empty search:     Sort 'search' with empty query falls back to 'hot'.
  No reddit_id:     Items with falsy id skipped before processing.
  Malformed items:  item.get() used defensively throughout collect_data.

detect_content_type note
------------------------
detect_content_type() in services/reddit_service.py uses getattr() and
expects a PRAW post object with attribute-style access. Calling it with a
plain dict silently returns 'text' for all posts. A dict-compatible version
_detect_content_type_from_dict() is therefore defined here. Any changes to
the PRAW version must be manually mirrored here.

Consumers
---------
  modules/reddit_scraper.py — instantiates RedditJSONService and calls
    collect_data() via JobManager when no PRAW credentials are present.
  scripts/scrub_deleted_data.py — does NOT call this service directly.
    Compliance scrubbing requires PRAW credentials; JSON endpoints have no
    equivalent of reddit.info(fullnames=...). See that script's module
    docstring for the documented limitation.
"""

import logging
import time
from datetime import datetime
from typing import Generator, Union

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from core.schemas import (
    CollectionParams,
    CollectionProgress,
    CollectionResult,
    CollectionStats,
    RateLimitConfig,
)
from services.reddit_service import (
    RateLimiter,
    detect_content_status,
    detect_language,
    generate_collection_hash,
    get_ppm_tags,
)
from utils.db_helpers import save_collected_data, update_scrape_run

logger = logging.getLogger(__name__)

# Hard cap on pagination — 10 pages × 100 posts = 1000 max per subreddit.
# Prevents infinite loops from stickied post cursor cycles or malformed responses.
# For a 150–200 post thesis, this limit is never reached in practice.
_MAX_PAGES = 10


# ---------------------------------------------------------------------------
# Retry decorator — mirrors reddit_retry in reddit_service.py
# ---------------------------------------------------------------------------

def _is_retryable_error(exc: BaseException) -> bool:
    """
    Predicate for tenacity retry: True if exc should be retried, False otherwise.
    SSLError is excluded because retrying won't fix certificate failures.
    """
    if isinstance(exc, requests.exceptions.SSLError):
        return False
    return isinstance(
        exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    )

def json_retry(func):
    """
    Tenacity retry decorator for TRANSIENT network failures only.

    Handles: ConnectionError, Timeout — both are worth retrying.
    Does NOT handle:
      - HTTP 429 — managed by _fetch_page's inner backoff loop
      - SSLError — structural; retrying won't fix a certificate problem
      - TooManyRedirects — structural; indicates a redirect loop

    Uses RateLimitConfig for consistent backoff settings with the PRAW service.
    before_sleep_log ensures retries are visible in logs.
    """
    config = RateLimitConfig()
    return retry(
        retry       = retry_if_exception(_is_retryable_error),
        stop        = stop_after_attempt(config.max_retries),
        wait        = wait_exponential(
            multiplier = config.backoff_base,
            min        = 4,
            max        = config.backoff_max,
        ),
        before_sleep = before_sleep_log(logger, logging.WARNING),
        reraise      = True,
    )(func)


# ---------------------------------------------------------------------------
# Content type detection — dict-compatible version
# ---------------------------------------------------------------------------

def _detect_content_type_from_dict(url: str, selftext: str, permalink: str) -> str:
    """
    Determine content type from raw JSON fields.

    This is the dict-compatible equivalent of detect_content_type() in
    reddit_service.py, which uses getattr() on PRAW post objects. Calling
    that function on a plain dict returns '' for all getattr() calls,
    making every post appear as 'text'. This version avoids that bug.

    Any changes to reddit_service.detect_content_type must be mirrored here.
    """
    if selftext and len(selftext) > 10:
        return 'text'
    elif any(ext in url.lower() for ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp')):
        return 'image'
    elif any(ext in url.lower() for ext in ('.mp4', '.webm', '.mov')):
        return 'video'
    elif 'reddit.com/gallery' in url or 'i.redd.it' in url:
        return 'image'
    elif 'v.redd.it' in url or 'youtube.com' in url or 'youtu.be' in url:
        return 'video'
    elif url and url != permalink:
        return 'link'
    return 'text'


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------

class RedditJSONService:
    """
    Collects Reddit posts via unauthenticated public JSON endpoints.

    Interface mirrors RedditService — JobManager and reddit_scraper.py
    treat both services interchangeably via duck typing.
    """

    def __init__(self, user_agent: str = None):
        """
        No credentials required.

        user_agent: Should match the format used by RedditService for
        consistency. Reddit blocks requests with generic or absent UA strings.
        Falls back to a descriptive default if not provided or too short.
        """
        if not user_agent or len(user_agent) < 10:
            self.user_agent = (
                "AcademicResearch:NootropicRedditScrapePPM:v1.0 (by /u/unknown)"
            )
        else:
            self.user_agent = user_agent

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

        # 30 req/min — more conservative than PRAW's 50 for unauthenticated access
        self.rate_limiter = RateLimiter(requests_per_minute=30)

    @json_retry
    def _fetch_page(self, subreddit: str, sort: str,
                    after: str | None, t: str = "all",
                    query: str = None) -> dict:
        """
        Fetch a single page of posts from Reddit's JSON endpoint.

        rate_limiter.wait() is called BEFORE every request attempt,
        including retries after a 429, so the rate limiter's accounting
        stays accurate across backoffs.

        Returns parsed JSON dict, or {} if the response is unparseable
        or has an unexpected schema. Caller must handle empty dict.

        Raises requests.exceptions.HTTPError for non-429 HTTP errors
        (403, 404, 5xx) after exhausting backoff for 429.
        """
        # URL and params assembly
        base_url = f"https://www.reddit.com/r/{subreddit}"
        req_params: dict = {"limit": 100}

        if after:
            req_params["after"] = after

        if sort == "search" and query:
            url = f"{base_url}/search.json"
            req_params["q"]           = query
            req_params["sort"]        = "relevance"
            req_params["restrict_sr"] = 1
        elif sort in ("top", "hot", "new"):
            url = f"{base_url}/{sort}.json"
            if sort == "top":
                req_params["t"] = t
        else:
            logger.warning("Unknown sort '%s' — falling back to hot.", sort)
            url = f"{base_url}/hot.json"

        # 429 backoff loop — separate from tenacity (which handles network errors)
        # rate_limiter.wait() fires before EVERY attempt including retries so
        # the 30 req/min budget stays accurate after a 429 backoff.
        backoffs = [5, 10, 20]
        attempts = 0

        # SSLError and TooManyRedirects are caught here so they can be logged
        # before propagating.  SSLError is also excluded from the @json_retry
        # decorator (it inherits from ConnectionError, which would otherwise be
        # retried).  TooManyRedirects is not a subclass of ConnectionError so
        # tenacity would not retry it regardless, but explicit handling adds
        # a useful log message.
        try:
            while True:
                self.rate_limiter.wait()   # always wait before each attempt
                response = self.session.get(url, params=req_params, timeout=10)

                if response.status_code == 429:
                    if attempts < len(backoffs):
                        sleep_time = backoffs[attempts]
                        logger.warning(
                            "429 rate limit on r/%s. Sleeping %ds (attempt %d/%d).",
                            subreddit, sleep_time, attempts + 1, len(backoffs),
                        )
                        time.sleep(sleep_time)
                        attempts += 1
                        continue
                    else:
                        logger.error(
                            "429 rate limit on r/%s — backoff exhausted after %d attempts.",
                            subreddit, len(backoffs),
                        )
                        response.raise_for_status()   # raises HTTPError — tenacity won't retry

                response.raise_for_status()   # raises HTTPError for 403, 404, 5xx
                break

        except requests.exceptions.SSLError as e:
            # Certificate invalid or TLS handshake failed.  Excluded from
            # @json_retry so this propagates immediately without retrying.
            logger.error("r/%s: SSL error — %s. Check network/certificate.", subreddit, e)
            raise
        except requests.exceptions.TooManyRedirects as e:
            # Redirect loop — typically unauthenticated redirect to login page.
            # Not a subclass of ConnectionError so tenacity would not retry it
            # anyway, but logging here makes the cause explicit.
            logger.error(
                "r/%s: too many redirects — Reddit may be redirecting to login. "
                "This subreddit may require authentication.", subreddit
            )
            raise

        # Schema validation — return {} on unexpected structure rather than crashing
        try:
            response_json = response.json()
        except ValueError:
            logger.warning("r/%s: response was not valid JSON — skipping page.", subreddit)
            return {}

        if "data" not in response_json or "children" not in response_json.get("data", {}):
            logger.warning(
                "r/%s: unexpected JSON schema (missing data.children) — skipping page.",
                subreddit,
            )
            return {}

        return response_json

    def fetch_posts(self, subreddit: str, sort: str = "top",
                    max_posts: int = 100, keyword: str = None,
                    date_after: float = 1577836800.0,
                    date_before: float = 1767225600.0) -> list[dict]:
        """
        Paginate the JSON endpoint and return raw intermediate item dicts.

        IMPORTANT — these dicts are NOT in final save_collected_data() format:
          - They contain a '_raw' key with the full JSON data blob
          - The 'metadata' dict contains only 'data_source' at this stage
          - 'text' contains full untruncated selftext
          - Content type, language, PPM tags, and filtering are NOT applied

        collect_data() calls this method and completes all processing.

        Edge cases handled here:
          - Null id/permalink/title/created_utc/selftext/score → safe defaults
          - Posts with no id are skipped (cannot be saved without reddit_id)
          - Stickied/duplicate posts tracked via seen_ids set
          - Hard page cap _MAX_PAGES prevents pagination loops
          - max_posts=0 guard (returns empty list cleanly)
          - search sort with empty keyword falls back to 'hot'

        Pagination terminates when:
          - The last post's created_utc < date_after (chronological boundary)
          - The 'after' cursor is None or absent (end of subreddit)
          - Total posts collected >= max_posts
          - Page count reaches _MAX_PAGES
        """
        # Guard: zero or negative limit returns immediately
        if max_posts <= 0:
            logger.warning("fetch_posts called with max_posts=%d — returning empty.", max_posts)
            return []

        # Guard: Search sort with empty/None keyword falls back to hot
        if sort == "search" and not (keyword and keyword.strip()):
            logger.warning(
                "Search sort requested but keyword is empty for r/%s — falling back to 'hot'.",
                subreddit,
            )
            sort = "hot"

        posts:       list  = []
        seen_ids:    set   = set()     # guard against stickied/duplicate posts
        after_cursor: str | None = None
        page_count:  int  = 0

        while len(posts) < max_posts and page_count < _MAX_PAGES:
            page_data = self._fetch_page(subreddit, sort, after_cursor, query=keyword)
            page_count += 1

            if not page_data:
                break

            children = page_data.get("data", {}).get("children", [])
            if not children:
                break

            last_created_utc: float | None = None
            page_had_new_posts = False

            for child in children:
                data = child.get("data", {})

                # Guard: posts without an id cannot be stored — skip
                post_id = data.get('id')
                if not post_id:
                    logger.debug("r/%s: post with no id encountered — skipping.", subreddit)
                    continue

                # Guard: deduplicate stickied posts and pagination overlaps
                if post_id in seen_ids:
                    logger.debug("r/%s: duplicate post %s — skipping.", subreddit, post_id)
                    continue
                seen_ids.add(post_id)

                # Guard: created_utc — float(None) raises TypeError
                raw_utc = data.get('created_utc')
                try:
                    created_utc = float(raw_utc) if raw_utc is not None else 0.0
                except (TypeError, ValueError):
                    logger.debug("r/%s: invalid created_utc %r — skipping.", subreddit, raw_utc)
                    continue

                last_created_utc = created_utc

                # Date boundary filter
                if created_utc < date_after or created_utc > date_before:
                    continue

                # Guard: permalink — 'https://reddit.com' + None raises TypeError
                raw_permalink = data.get('permalink') or ''
                permalink = "https://reddit.com" + raw_permalink

                posts.append({
                    '_raw':         data,
                    'id':           post_id,
                    'type':         'submission',
                    'subreddit':    data.get('subreddit') or subreddit,
                    'title':        data.get('title') or '',           # None → ''
                    'text':         data.get('selftext') or '',        # None → '' (link posts)
                    'author':       data.get('author') or '[deleted]', # None → '[deleted]'
                    'score':        data.get('score') or 0,            # None → 0
                    'created_utc':  created_utc,
                    'num_comments': data.get('num_comments') or 0,
                    'url':          data.get('url') or '',
                    'permalink':    permalink,
                    'post_id':      None,
                    'data_source':  'json_endpoint',
                    'metadata':     {'data_source': 'json_endpoint'},
                })
                page_had_new_posts = True

                if len(posts) >= max_posts:
                    break

            # If the entire page had no new posts (all stickied/seen), stop
            if not page_had_new_posts:
                logger.info(
                    "r/%s: page %d had no new posts (all duplicates/stickied) — stopping.",
                    subreddit, page_count,
                )
                break

            after_cursor = page_data.get("data", {}).get("after")

            if not after_cursor:
                break

            if last_created_utc is not None and last_created_utc < date_after:
                logger.info(
                    "r/%s: reached date boundary at page %d — stopping.",
                    subreddit, page_count,
                )
                break

        if page_count >= _MAX_PAGES:
            logger.warning(
                "r/%s: hit _MAX_PAGES limit (%d) — %d posts collected. "
                "Consider reducing params.limit.",
                subreddit, _MAX_PAGES, len(posts),
            )

        return posts

    def verify_connection(self) -> bool:
        """
        Lightweight connectivity check — fetches 1 post from r/Nootropics.
        Returns True on success, False on any error.
        Used by modules/reddit_scraper.py to show connection status.
        """
        try:
            result = self._fetch_page("Nootropics", sort="top", after=None)
            return bool(result)
        except Exception as e:
            logger.error("JSON endpoint verification failed: %s", e)
            return False

    def collect_data(
        self, params: CollectionParams
    ) -> Generator[Union[CollectionProgress, CollectionResult], None, None]:
        """
        Collect posts from all target subreddits and yield progress + result.

        Mirrors RedditService.collect_data() — yielded types are identical
        so JobManager and reddit_scraper.py require no changes.

        Generator protocol (detected by JobManager via hasattr):
          - Yields CollectionProgress throughout (hasattr 'progress_percentage')
          - Yields CollectionResult once at the end (hasattr 'collection_hash')

        Comment collection (params.collect_comments) is silently ignored —
        the JSON endpoint does not support comment-level pagination in this
        implementation. The flag is set in the validation dict so the UI
        can surface this limitation.

        Incremental saves: items are written to DB in 50-item batches.
        collected_posts buffer is cleared after each save. CollectionResult
        always has empty collected_posts — use stats.total_collected for count.
        """
        if not params.collection_started:
            params.collection_started = datetime.utcnow().isoformat()

        # Retrieve job tracking fields set by job_manager after instantiation
        session_id = getattr(params, 'session_id', None)
        job_id     = getattr(params, 'job_id',     None)

        if not session_id:
            logger.warning(
                "collect_data: session_id is None — records will be stored with "
                "NULL session_id and may not appear in session-filtered queries."
            )

        # Hash computed AFTER params are fully set (includes job_id if present)
        # — consistent with how reddit_service.py computes it.
        params_dict      = params.model_dump()
        collection_hash  = generate_collection_hash(params_dict)

        if params.collect_comments:
            logger.warning(
                "collect_comments=True ignored — JSON endpoint does not support "
                "comment-level collection."
            )

        collected_posts:  list  = []
        total_saved_count: int  = 0
        stats              = CollectionStats()
        rate_limit_events: list = []
        total_subreddits   = len(params.subreddits)

        for idx, subreddit_name in enumerate(params.subreddits):
            yield CollectionProgress(
                current_subreddit    = subreddit_name,
                progress_percentage  = idx / max(total_subreddits, 1),
                status_message       = f"Fetching r/{subreddit_name} (JSON endpoint)...",
                rate_stats           = self.rate_limiter.get_stats(),
            )

            try:
                method_map = {
                    "Recent Posts (Hot)":    "hot",
                    "Recent Posts (New)":    "new",
                    "Top Posts (Time Period)": "top",
                    "Search Query":          "search",
                }
                sort_method = method_map.get(params.method, "hot")

                # Pass date boundaries from CollectionParams — not hardcoded defaults
                # Guard: limit=0 → max(params.limit, 1) prevents empty loop
                # Guard: empty search query with Search method → fetch_posts falls back to 'hot'
                fetched_items = self.fetch_posts(
                    subreddit   = subreddit_name,
                    sort        = sort_method,
                    max_posts   = max(params.limit, 1),
                    keyword     = params.search_query,
                    date_after  = params.date_after,
                    date_before = params.date_before,
                )

                for item in fetched_items:
                    # Pop raw JSON data — used only for derived fields below.
                    # Not part of the save_collected_data() contract.
                    raw = item.pop('_raw', {})

                    # Guard: item with no id should never reach here (fetch_posts filters them)
                    # but check defensively — save_collected_data requires reddit_id
                    if not item.get('id'):
                        logger.warning("collect_data: item with no id encountered — skipping.")
                        continue

                    subreddit_nsfw = raw.get('over18', False)
                    post_nsfw      = raw.get('over_18', False)

                    if post_nsfw and not params.include_nsfw:
                        stats.skipped_nsfw += 1
                        continue

                    # Truncation — capture original length BEFORE modifying item['text']
                    # Use .get() defensively — item dict could be malformed
                    original_text   = item.get('text') or ''
                    post_text       = original_text[:params.max_text_length] if original_text else ''
                    word_count      = len(post_text.split())
                    was_truncated   = len(original_text) > params.max_text_length if original_text else False

                    # Update item['text'] to the truncated version — matches PRAW service behaviour
                    item['text'] = post_text

                    if word_count < params.min_word_count_val:
                        continue

                    # Use .get() for author defensively
                    author = item.get('author') or '[deleted]'
                    content_status = detect_content_status(post_text, author)

                    if content_status in ('removed', 'author_deleted') and not params.include_removed:
                        stats.skipped_removed += 1
                        continue

                    # Dict-compatible content type detection (see module docstring)
                    content_type = _detect_content_type_from_dict(
                        url       = item.get('url') or '',
                        selftext  = post_text,
                        permalink = item.get('permalink') or '',
                    )

                    if (content_type in ('image', 'video', 'link')
                            and not post_text
                            and not params.include_media_only):
                        stats.skipped_media_only += 1
                        continue

                    language = detect_language(post_text) if params.flag_non_english else 'not_checked'
                    if language == 'likely_non_english':
                        stats.flagged_non_english += 1

                    title = item.get('title') or ''
                    auto_tags = get_ppm_tags(f"{title} {post_text}")

                    # Complete the metadata dict — matches RedditItemMetadata shape
                    item['metadata'].update({
                        'nsfw':                  post_nsfw,
                        'subreddit_nsfw':        subreddit_nsfw,
                        'content_status':        content_status,
                        'content_type':          content_type,
                        'language_flag':         language,
                        'text_length':           len(post_text),
                        'word_count':            word_count,
                        'auto_tags':             auto_tags,
                        'was_truncated':         was_truncated,
                        'upvote_ratio':          raw.get('upvote_ratio'),
                        'is_original_content':   raw.get('is_original_content', False),
                        'is_crosspostable':      raw.get('is_crosspostable', True),
                        'spoiler':               raw.get('spoiler', False),
                        'stickied':              raw.get('stickied', False),
                        'locked':                raw.get('locked', False),
                        'collection_hash':       collection_hash,
                        'flair_text':            raw.get('link_flair_text'),
                    })
                    item['title']        = title       # ensure .get() version is stored
                    item['author']       = author      # ensure .get() version is stored
                    item['collected_at'] = datetime.utcnow().isoformat()

                    collected_posts.append(item)

                    # Incremental save — 50-item checkpoint matching PRAW service
                    if len(collected_posts) >= 50:
                        saved = save_collected_data(collected_posts, session_id)
                        total_saved_count += saved
                        collected_posts.clear()
                        if job_id:
                            update_scrape_run(job_id, items_collected=total_saved_count)

                    yield CollectionProgress(
                        current_subreddit   = subreddit_name,
                        progress_percentage = idx / max(total_subreddits, 1),
                        status_message      = f"Processing r/{subreddit_name} (JSON)...",
                        rate_stats          = self.rate_limiter.get_stats(),
                    )

            except requests.exceptions.HTTPError as e:
                # 403 (private), 404 (not found), 5xx — skip subreddit
                logger.warning("r/%s: HTTP error %s — skipping.", subreddit_name, e)
                rate_limit_events.append({
                    'type':      'http_error',
                    'subreddit': subreddit_name,
                    'error':     str(e),
                    'timestamp': datetime.utcnow().isoformat(),
                })
                yield CollectionProgress(
                    current_subreddit   = subreddit_name,
                    progress_percentage = idx / max(total_subreddits, 1),
                    status_message      = f"Skipped r/{subreddit_name}: {e}",
                    rate_stats          = self.rate_limiter.get_stats(),
                )

            except Exception as e:
                logger.error("r/%s: unexpected error — %s", subreddit_name, e)
                rate_limit_events.append({
                    'type':      'json_fetch_error',
                    'subreddit': subreddit_name,
                    'error':     str(e),
                    'timestamp': datetime.utcnow().isoformat(),
                })
                yield CollectionProgress(
                    current_subreddit   = subreddit_name,
                    progress_percentage = idx / max(total_subreddits, 1),
                    status_message      = f"Error on r/{subreddit_name}: {e}",
                    rate_stats          = self.rate_limiter.get_stats(),
                )

        # Final flush of any remaining buffer
        if collected_posts:
            saved = save_collected_data(collected_posts, session_id)
            total_saved_count += saved
            collected_posts.clear()
            if job_id:
                update_scrape_run(job_id, items_collected=total_saved_count)

        # Build stats — nsfw_subreddits always [] (buffer cleared above)
        # See CollectionStats docstring for full explanation.
        stats.total_collected      = total_saved_count
        stats.nsfw_collected       = 0  # NOT POPULATED — buffer cleared
        stats.removed_collected    = 0  # NOT POPULATED — buffer cleared
        stats.truncated_collected  = 0  # NOT POPULATED — buffer cleared
        stats.media_only_collected = 0  # NOT POPULATED — buffer cleared
        stats.non_english_collected = stats.flagged_non_english
        stats.nsfw_subreddits      = []  # NOT POPULATED — buffer cleared
        stats.nsfw_subreddits_count = 0

        validation = {
            'nsfw_collected':        stats.nsfw_collected,
            'removed_collected':     stats.removed_collected,
            'truncated_collected':   stats.truncated_collected,
            'media_only_collected':  stats.media_only_collected,
            'non_english_collected': stats.flagged_non_english,
            'nsfw_subreddits':       stats.nsfw_subreddits,
            'comments_collected':    False,   # JSON endpoint — comments not supported
            'collect_comments_requested': params.collect_comments,
        }

        yield CollectionProgress(
            current_subreddit   = "Done",
            progress_percentage = 1.0,
            status_message      = f"Collection complete — {total_saved_count} items saved.",
        )

        yield CollectionResult(
            collection_hash      = collection_hash,
            collected_posts      = [],    # always empty — data saved incrementally
            rate_limit_events    = rate_limit_events,
            stats                = stats,
            validation           = validation,
            collection_started   = params.collection_started,
            collection_completed = datetime.utcnow().isoformat(),
        )
