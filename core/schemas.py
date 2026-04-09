"""
core/schemas.py
===============
Pydantic v2 data models for NootropicRedditScrapePPM.

Runtime usage notes
-------------------
- RedditCredentials     — instantiated in modules/reddit_scraper.py and services/reddit_service.py
- RateLimitConfig       — instantiated in services/reddit_service.py (defaults only)
- CollectionParams      — instantiated in modules/reddit_scraper.py, consumed by both services + job_manager
- CollectionProgress    — yielded by both services, consumed by job_manager (hasattr duck-typing)
- CollectionResult      — yielded by both services, consumed by job_manager (hasattr duck-typing)
- CollectionStats       — built inside both services, embedded in CollectionResult
- JobStatus / JobState  — managed exclusively by services/job_manager.py
- RateLimitEvent        — reference schema only; rate_limit_events passed as List[Dict] in practice
- CollectedItem            — reference schema only; items passed as raw dicts throughout the pipeline
- ItemMetadata    — reference schema only; metadata embedded in item dict under 'metadata' key

Pydantic version: >=2.0.0 (see pyproject.toml)

Migration notes
---------------
- RateLimitConfig.class Config removed (was Pydantic v1 syntax, pydantic-settings not installed)
- HttpUrl import removed (was imported but never used)
- data_source added to CollectedItem and ItemMetadata
- date_after / date_before added to CollectionParams for JSON endpoint date range filtering
- CollectionStats fields with known zero-population documented inline
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class RedditCredentials(BaseModel):
    """
    OAuth credentials for PRAW (script-type app).
    """
    client_id:     str
    client_secret: str
    user_agent:    str


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

class RateLimitConfig(BaseModel):
    """
    Rate limiting parameters for RedditService (PRAW). Instantiated with defaults
    — no env override configured (pydantic-settings not installed; use os.getenv if needed).

    PRAW service uses requests_per_minute=50 (Reddit OAuth cap).
    """
    requests_per_minute: int   = Field(default=50,  ge=1, le=60,  description="Target requests per minute")
    max_retries:         int   = Field(default=5,   ge=1, le=10,  description="Retry attempts on rate limit / connection error")
    backoff_base:        float = Field(default=2.0,               description="Exponential backoff multiplier (seconds)")
    backoff_max:         int   = Field(default=120,               description="Maximum backoff ceiling in seconds")


# ---------------------------------------------------------------------------
# Collection parameters
# ---------------------------------------------------------------------------

class CollectionParams(BaseModel):
    """
    Parameters for a single collection run.
    Passed from modules/reddit_scraper.py → JobManager → service.collect_data().

    job_id and session_id are set by job_manager.py after instantiation:
        params.job_id = job_id
        (session_id is set by reddit_scraper.py before passing to JobManager)

    Both services read these via direct attribute access (job_id is Optional
    so `if params.job_id:` is sufficient — hasattr guards in reddit_service.py
    are now redundant but harmless).

    RedditService (PRAW) relies on time_filter instead.
    """
    # Core collection parameters
    subreddits:        List[str]
    method:            str                       # 'Recent Posts (Hot)' | 'Recent Posts (New)' | 'Top Posts (Time Period)' | 'Search Query'
    time_filter:       str                       # 'day' | 'week' | 'month' | 'year' | 'all' — PRAW only
    limit:             int
    search_query:      Optional[str]   = None

    # Comment collection — PRAW only
    collect_comments:  bool  = True
    comment_limit:     int   = 50

    # Content filtering
    include_nsfw:       bool = False
    include_removed:    bool = True
    include_media_only: bool = True
    flag_non_english:   bool = True
    max_text_length:    int  = 50000
    min_word_count_val: int  = 0

    # Date range filtering (Unix timestamps)
    # Default: 2020-01-01 00:00:00 UTC → 2025-12-31 23:59:59 UTC
    date_after:  float = Field(default=1577836800.0, description="Unix timestamp — posts before this date excluded")
    date_before: float = Field(default=1893456000.0, description="Unix timestamp — posts after this date excluded. Default: 2030-01-01")

    # Set by modules/reddit_scraper.py before job start
    user_agent:          Optional[str] = None
    collection_started:  Optional[str] = None
    session_label:       Optional[str] = None

    # Set by job_manager.py after instantiation — not passed from UI
    job_id:     Optional[str] = None
    session_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Item metadata and item shape (reference schemas — not used as runtime validators)
# ---------------------------------------------------------------------------

class ItemMetadata(BaseModel):
    """
    Reference schema for the 'metadata' dict embedded in each item dict.

    NOT instantiated in the pipeline — items are constructed as plain dicts
    in both services. This schema documents the expected metadata shape for
    type-checking tools.
    """
    # Content classification
    nsfw:                  bool              = False
    subreddit_nsfw:        Optional[bool]    = False
    content_status:        str               = 'available'   # 'available' | 'removed' | 'author_deleted' | 'empty'
    content_type:          Optional[str]     = None          # 'text' | 'image' | 'video' | 'link'
    language_flag:         str               = 'not_checked' # 'english' | 'likely_non_english' | 'not_checked'

    # Text stats
    text_length:           int               = 0
    word_count:            Optional[int]     = None
    was_truncated:         bool              = False

    # PPM tagging
    auto_tags:             Optional[List[str]] = None        # PPM category tags from keyword match

    # Post attributes
    upvote_ratio:          Optional[float]   = None
    is_original_content:   Optional[bool]    = None
    is_crosspostable:      Optional[bool]    = None
    spoiler:               Optional[bool]    = None
    stickied:              Optional[bool]    = None
    locked:                Optional[bool]    = None
    flair_text:            Optional[str]     = None

    # Collection audit
    collection_hash:       str               = ''
    data_source:           Optional[str]     = 'praw'

    # Comment-only fields (None for submissions)
    is_submitter:          Optional[bool]    = None
    depth:                 Optional[int]     = None
    parent_id:             Optional[str]     = None


class CollectedItem(BaseModel):
    """
    Reference schema for the item dicts passed to save_collected_data().

    NOT instantiated in the pipeline — items are constructed as plain dicts
    in both services. This schema documents the exact dict shape that
    save_collected_data() in utils/db_helpers.py expects.

    Key fields:
      id          — bare reddit ID (e.g. 'abc123') — NOT the t3_ fullname
      text        — post body / comment body (field name is 'text', NOT 'body')
      data_source — 'praw' — written to collected_data.data_source
      metadata    — nested dict matching ItemMetadata shape
    """
    id:           str
    type:         str                        # 'submission' | 'comment'
    subreddit:    str
    title:        str
    text:         str                        # selftext for submissions, body for comments — always 'text'
    author:       str
    score:        int
    created_utc:  float
    num_comments: Optional[int]   = None
    url:          Optional[str]   = None
    permalink:    str                        # full URL: 'https://reddit.com/r/...'
    post_id:      Optional[str]   = None     # None for top-level submissions
    collected_at: str                        # ISO format datetime string
    data_source:  str             = 'praw'
    metadata:     Dict[str, Any]  = {}       # shape matches ItemMetadata

    @field_validator('author')
    @classmethod
    def validate_author_pii(cls, v: str) -> str:
        if v.lower() in ["[deleted]", "[removed]"]:
            return v
        if len(v) >= 32 and all(c in "0123456789abcdefABCDEF" for c in v):
            return v
        if v.startswith("anon_") or v.startswith("pseudonym_"):
            return v
        raise ValueError(
            "Author field MUST be pseudonymized (e.g. SHA256 hashed, 'anon_XXX', or '[deleted]') "
            "to comply with strict PII constraints for reusable analysis."
        )


# ---------------------------------------------------------------------------
# Rate limit events (reference schema — List[Dict] used in practice)
# ---------------------------------------------------------------------------

class RateLimitEvent(BaseModel):
    """
    Reference schema for rate limit / error events collected during a run.

    NOTE: CollectionResult.rate_limit_events is typed as List[Dict[str, Any]]
    for backward compatibility — events are constructed as plain dicts in both
    services. This schema documents the expected dict shape.

    Expected keys that both services must populate:
      type       — 'rate_limit_hard' | 'rate_limit_429' | 'comment_fetch_error' | 'page_schema_error'
      subreddit  — subreddit name where the event occurred
      error      — error message string
      timestamp  — ISO format datetime string
    """
    type:      str
    subreddit: Optional[str] = None
    post_id:   Optional[str] = None
    error:     str
    timestamp: str


# ---------------------------------------------------------------------------
# Collection statistics
# ---------------------------------------------------------------------------

class CollectionStats(BaseModel):
    """
    Statistics accumulated during a collection run.

    Fields marked [POPULATED] are set with real values.
    Fields marked [NOT POPULATED — always 0] are never updated due to
    incremental saving clearing the buffer before final stats computation.
    These are retained for schema stability (existing replicability_log
    records include these fields as JSON).
    """
    # [POPULATED] — accurate values
    total_collected:       int        = 0
    skipped_nsfw:          int        = 0
    skipped_removed:       int        = 0
    skipped_media_only:    int        = 0
    flagged_non_english:   int        = 0

    # [NOT POPULATED — always 0]
    # Reason: computed from collected_posts buffer which is cleared before
    # final stats. Requires a post-run DB query to derive accurately.
    # TODO: derive from DB query after collection completes.
    nsfw_collected:        int        = 0
    removed_collected:     int        = 0
    truncated_collected:   int        = 0
    media_only_collected:  int        = 0
    non_english_collected: int        = 0

    # [NOT POPULATED — always empty list]
    # Reason: same buffer-clearing issue as above.
    nsfw_subreddits:       List[str]  = []
    nsfw_subreddits_count: int        = 0


# ---------------------------------------------------------------------------
# Generator yield types
# ---------------------------------------------------------------------------

class CollectionProgress(BaseModel):
    """
    Yielded by both services during collection.
    Detected by job_manager.py via: hasattr(item, 'progress_percentage')
    """
    current_subreddit:    str
    progress_percentage:  float               # 0.0 – 1.0
    status_message:       str
    rate_stats:           Optional[Dict[str, Any]] = None
    eta_seconds:          Optional[float]     = None


class CollectionResult(BaseModel):
    """
    Yielded once by both services at the end of collect_data().
    Detected by job_manager.py via: hasattr(item, 'collection_hash')

    IMPORTANT — collected_posts is ALWAYS an empty list ([]).
    Both services save data incrementally in 50-item chunks during collection
    and clear the buffer after each save. By the time CollectionResult is
    yielded, no posts remain in the buffer.

    Do NOT attempt to save or count from collected_posts in the UI handler.
    Use final_result.stats.total_collected for the item count instead.
    Use load_collected_data() to refresh session state after completion.

    rate_limit_events is List[Dict] for backward compatibility.
    See RateLimitEvent for the expected dict shape.
    """
    collection_hash:   str
    collected_posts:   List[Dict[str, Any]]  # always [] — see docstring above
    rate_limit_events: List[Dict[str, Any]]  # shape: see RateLimitEvent
    stats:             CollectionStats
    validation:        Dict[str, Any]
    collection_started:   str               # ISO format datetime string
    collection_completed: str               # ISO format datetime string


# ---------------------------------------------------------------------------
# Job management
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    """
    Lifecycle states for a background collection job.
    Persisted to scrape_runs.status in the database.
    """
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    CANCELLED = "CANCELLED"


class JobState(BaseModel):
    """
    In-memory state for a running job managed by JobManager.
    Not persisted to DB — the DB equivalent is ScrapeRun in core/database.py.
    """
    job_id:       str
    status:       JobStatus              = JobStatus.PENDING
    progress:     Optional[CollectionProgress] = None
    result:       Optional[CollectionResult]   = None
    error:        Optional[str]          = None
    started_at:   Optional[str]          = None
    completed_at: Optional[str]          = None
