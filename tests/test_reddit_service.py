import pytest
from unittest.mock import Mock, patch
import time
import prawcore

from services.reddit_service import RateLimiter, RedditService
from core.schemas import RedditCredentials, RateLimitConfig

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
