import pytest
from unittest.mock import Mock, patch
from services.reddit_service import (
    detect_content_status,
    detect_language,
    detect_content_type,
    generate_collection_hash,
    get_ppm_tags
)

def test_detect_content_status():
    assert detect_content_status('[removed]', 'some_user') == 'removed'
    assert detect_content_status('[deleted]', 'some_user') == 'removed'
    assert detect_content_status('regular text', '[deleted]') == 'author_deleted'
    assert detect_content_status('regular text', None) == 'author_deleted'
    assert detect_content_status('', 'some_user') == 'empty'
    assert detect_content_status('   ', 'some_user') == 'empty'
    assert detect_content_status('regular text', 'some_user') == 'available'

def test_detect_language():
    assert detect_language('This is a normal english sentence') == 'english'
    assert detect_language('') == 'unknown'
    assert detect_language(None) == 'unknown'
    
    # Text with >30% non-ascii
    non_ascii_text = "This is English with some foreign characters 測試測試測試測試測試測試測試測試測試測試測試測試測試測試"
    assert detect_language(non_ascii_text) == 'likely_non_english'
    
    ascii_dominant = "Mostly english 測試"
    assert detect_language(ascii_dominant) == 'english'

def test_detect_content_type():
    MockPost = Mock()
    
    # Text post
    MockPost.url = 'https://reddit.com/r/test/comments/123/test/'
    MockPost.selftext = 'This is a long enough self text to be considered text.'
    assert detect_content_type(MockPost) == 'text'
    
    # Image post
    MockPost.selftext = ''
    MockPost.url = 'https://i.redd.it/image.png'
    assert detect_content_type(MockPost) == 'image'
    
    MockPost.url = 'https://reddit.com/gallery/123'
    assert detect_content_type(MockPost) == 'image'
    
    # Video post
    MockPost.url = 'https://v.redd.it/video123'
    assert detect_content_type(MockPost) == 'video'
    
    MockPost.url = 'https://example.com/video.mp4'
    assert detect_content_type(MockPost) == 'video'
    
    # Link post
    MockPost.url = 'https://external-link.com/article'
    MockPost.permalink = '/r/test/comments/123/'
    assert detect_content_type(MockPost) == 'link'

def test_generate_collection_hash():
    params1 = {"a": 1, "b": "test"}
    params2 = {"b": "test", "a": 1}
    params3 = {"a": 2, "b": "test"}
    
    # Should be deterministic regardless of dictionary key order
    assert generate_collection_hash(params1) == generate_collection_hash(params2)
    assert generate_collection_hash(params1) != generate_collection_hash(params3)
    
    # Length should be exactly 16 chars
    assert len(generate_collection_hash(params1)) == 16

@patch('services.reddit_service.get_ppm_keywords')
def test_get_ppm_tags(mock_get_ppm_keywords):
    mock_get_ppm_keywords.return_value = {
        'Push': ['stress', 'pain'],
        'Pull': ['performance', 'focus'],
        'Mooring': ['expensive', 'side effect']
    }
    
    # Matches Push
    assert 'Push' in get_ppm_tags('I have a lot of stress lately')
    
    # Matches Multiple
    tags = get_ppm_tags('I want better performance but it has a side effect')
    assert 'Pull' in tags
    assert 'Mooring' in tags
    assert 'Push' not in tags
    
    # Empty/None
    assert get_ppm_tags('') == []
    assert get_ppm_tags(None) == []
    
    # No matches
    assert get_ppm_tags('Just a normal sentence') == []
