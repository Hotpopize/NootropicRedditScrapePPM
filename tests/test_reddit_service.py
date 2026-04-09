import pytest
from unittest.mock import Mock, patch
import time
import prawcore

from services.reddit_service import RateLimiter, RedditService
from core.schemas import RedditCredentials, RateLimitConfig, CollectionParams

class TestRateLimiter:
    def test_respects_minimum_interval(self):
        limiter = RateLimiter(requests_per_minute=60)
        
        start = time.time()
        limiter.wait()
        limiter.wait()
        elapsed = time.time() - start
        
        assert elapsed >= 1.0  # At least 1 second for 2 requests at 60 rpm
    
    @patch('time.sleep')
    def test_resets_window_after_60_seconds(self, mock_sleep):
        limiter = RateLimiter(requests_per_minute=2)
        
        limiter.wait()
        limiter.wait()
        assert limiter.request_count == 2
        
        # Simulate time passing
        limiter.window_start -= 61
        limiter.wait()
        
        assert limiter.request_count == 1  # Reset occurred
    
    @patch('time.sleep')
    def test_sleeps_when_limit_reached(self, mock_sleep):
        limiter = RateLimiter(requests_per_minute=2)
        limiter.request_count = 2
        limiter.window_start = time.time()
        
        limiter.wait()
        
        mock_sleep.assert_called()

@patch('praw.Reddit')
def test_verify_credentials_success(mock_praw):
    mock_reddit = Mock()
    mock_reddit.user.me.return_value = "test_user"
    mock_praw.return_value = mock_reddit

    creds = RedditCredentials(client_id="id", client_secret="secret", user_agent="test")
    service = RedditService(creds)
    
    # Just verify the credentials verify successfully
    assert service.verify_credentials() is True



@patch('praw.Reddit')
def test_collect_data_yields(mock_praw):
    mock_reddit = Mock()
    mock_sub = Mock()
    
    mock_post = Mock()
    mock_post.id = "1"
    mock_post.title = "Test"
    mock_post.selftext = "Text"
    mock_post.author = "user"
    mock_post.created_utc = 1600000000.0
    mock_post.url = "http://test"
    mock_post.permalink = "/r/test"
    mock_post.subreddit = Mock()
    mock_post.subreddit.display_name = "Nootropics"
    mock_post.num_comments = 0
    mock_post.score = 1
    mock_post.over_18 = False
    
    mock_sub.hot.return_value = [mock_post]
    mock_reddit.subreddit.return_value = mock_sub
    mock_praw.return_value = mock_reddit
    
    creds = RedditCredentials(client_id="id", client_secret="secret", user_agent="test")
    service = RedditService(creds)
    params = CollectionParams(subreddits=["Nootropics"], method="Recent Posts (Hot)", time_filter="all", limit=1, search_query="")
    
    with patch('services.reddit_service.save_collected_data'):
        with patch('services.reddit_service.update_scrape_run'):
            results = list(service.collect_data(params))
            assert len(results) >= 2
            assert hasattr(results[-1], 'collection_hash')
