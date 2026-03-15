from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

class RedditCredentials(BaseModel):
    client_id: str
    client_secret: str
    user_agent: str

class RateLimitConfig(BaseModel):
    """Reddit API rate limiting configuration."""
    requests_per_minute: int = Field(default=50, ge=1, le=60, description="Target requests per minute")
    max_retries: int = Field(default=5, ge=1, le=10, description="Retry attempts on rate limit")
    backoff_base: float = Field(default=2.0, description="Exponential backoff multiplier")
    backoff_max: int = Field(default=120, description="Maximum backoff wait in seconds")
    
    class Config:
        env_prefix = "REDDIT_RATELIMIT_"

class CollectionParams(BaseModel):
    subreddits: List[str]
    method: str
    time_filter: str
    limit: int
    search_query: Optional[str] = None
    collect_comments: bool = True
    comment_limit: int = 50
    include_nsfw: bool = False
    include_removed: bool = True
    include_media_only: bool = True
    flag_non_english: bool = True
    max_text_length: int = 50000
    collection_started: Optional[str] = None
    user_agent: Optional[str] = None
    min_word_count_val: int = 0
    job_id: Optional[str] = None
    session_id: Optional[str] = None

class RedditItemMetadata(BaseModel):
    nsfw: bool = False
    subreddit_nsfw: Optional[bool] = False
    content_status: str
    content_type: Optional[str] = None
    language_flag: str
    text_length: int
    word_count: Optional[int] = None
    auto_tags: Optional[List[str]] = None
    was_truncated: bool = False
    upvote_ratio: Optional[float] = None
    is_original_content: Optional[bool] = None
    is_crosspostable: Optional[bool] = None
    spoiler: Optional[bool] = None
    stickied: Optional[bool] = None
    locked: Optional[bool] = None
    collection_hash: str
    flair_text: Optional[str] = None
    is_submitter: Optional[bool] = None
    depth: Optional[int] = None
    parent_id: Optional[str] = None

class RedditItem(BaseModel):
    id: str
    type: str  # 'submission' or 'comment'
    subreddit: str
    title: str
    text: str
    author: str
    score: int
    created_utc: float
    num_comments: Optional[int] = None
    url: Optional[str] = None
    permalink: str
    post_id: Optional[str] = None
    collected_at: str
    metadata: RedditItemMetadata

class RateLimitEvent(BaseModel):
    type: str
    subreddit: Optional[str] = None
    post_id: Optional[str] = None
    error: str
    timestamp: str

class CollectionStats(BaseModel):
    total_collected: int = 0
    skipped_nsfw: int = 0
    skipped_removed: int = 0
    skipped_media_only: int = 0
    flagged_non_english: int = 0
    nsfw_collected: int = 0
    removed_collected: int = 0
    truncated_collected: int = 0
    media_only_collected: int = 0
    non_english_collected: int = 0
    nsfw_subreddits: List[str] = []
    nsfw_subreddits_count: int = 0

class CollectionProgress(BaseModel):
    current_subreddit: str
    progress_percentage: float
    status_message: str
    rate_stats: Optional[Dict[str, Any]] = None
    eta_seconds: Optional[float] = None

class CollectionResult(BaseModel):
    collection_hash: str
    collected_posts: List[Dict[str, Any]] # raw dictionary elements ready for DB
    rate_limit_events: List[Dict[str, Any]]
    stats: CollectionStats
    validation: Dict[str, Any]
    collection_started: str
    collection_completed: str


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class JobState(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.PENDING
    progress: Optional[CollectionProgress] = None
    result: Optional[CollectionResult] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
