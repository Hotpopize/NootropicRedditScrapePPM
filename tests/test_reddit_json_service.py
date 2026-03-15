import pytest
from services.reddit_json_service import RedditJSONService

def test_fetch_posts():
    """Integration test to verify non-authenticated collection structure."""
    service = RedditJSONService(user_agent="AcademicResearch:NootropicsJSONTest:v1.0")
    
    # Verify connection first
    assert service.verify_connection() is True, "Connection verification failed"
    
    # Fetch 5 posts from r/Nootropics
    posts = service.fetch_posts(subreddit="Nootropics", sort="top", max_posts=5)
    
    assert len(posts) > 0, "No posts returned"
    
    for item in posts:
        assert 'id' in item
        assert 'subreddit' in item
        assert item['data_source'] == 'json_endpoint'
        
        # Metadata checks
        assert 'metadata' in item
        assert item['metadata']['data_source'] == 'json_endpoint'
        
        # Check that it's a bare ID without t3_
        assert not item['id'].startswith('t3_')
        
        # Check URL structure
        assert item['permalink'].startswith('https://reddit.com/r/')
