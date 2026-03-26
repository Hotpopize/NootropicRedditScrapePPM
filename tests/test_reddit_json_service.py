import pytest
from unittest.mock import patch, MagicMock
from services.reddit_json_service import RedditJSONService, _detect_content_type_from_dict
from core.schemas import CollectionParams

def test_detect_content_type_from_dict():
    assert _detect_content_type_from_dict("https://reddit.com/r/test", "This is some selftext that is quite long", "/r/test") == "text"
    assert _detect_content_type_from_dict("https://i.redd.it/image.jpg", "", "") == "image"
    assert _detect_content_type_from_dict("https://v.redd.it/video", "", "") == "video"
    assert _detect_content_type_from_dict("https://outbound.com", "", "/r/test") == "link"

@patch('requests.Session.get')
def test_verify_connection(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"children": [{"data": {"id": "123"}}]}}
    mock_get.return_value = mock_response

    service = RedditJSONService(user_agent="TestAgent:v1")
    assert service.verify_connection() is True

@patch('requests.Session.get')
def test_verify_connection_failure(mock_get):
    mock_get.side_effect = Exception("Connection error")
    service = RedditJSONService(user_agent="TestAgent:v1")
    assert service.verify_connection() is False

@patch('requests.Session.get')
def test_fetch_posts_empty(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {}
    mock_get.return_value = mock_response

    service = RedditJSONService(user_agent="TestAgent:v1")
    posts = service.fetch_posts(subreddit="Nootropics", max_posts=10)
    assert len(posts) == 0

@patch('requests.Session.get')
def test_fetch_posts_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {
            "children": [
                {"data": {"id": "1", "created_utc": 1600000000.0, "title": "Test 1"}},
                {"data": {"id": "2", "created_utc": 1600000001.0, "title": "Test 2"}}
            ]
        }
    }
    mock_get.return_value = mock_response

    service = RedditJSONService(user_agent="TestAgent:v1")
    posts = service.fetch_posts(subreddit="Nootropics", sort="hot", max_posts=2)
    assert len(posts) == 2
    assert posts[0]['id'] == "1"
    assert posts[1]['id'] == "2"
    assert posts[0]['data_source'] == "json_endpoint"

def test_collect_data_yields():
    service = RedditJSONService(user_agent="TestAgent:v1")
    params = CollectionParams(
        subreddits=["Nootropics"],
        method="Recent Posts (Hot)",
        time_filter="all",
        limit=2,
        search_query="",
    )
    
    with patch.object(service, 'fetch_posts') as mock_fetch:
        with patch('services.reddit_json_service.save_collected_data', return_value=1):
            mock_fetch.return_value = [
                {"id": "1", "title": "Test 1", "text": "Some text", "author": "user1", "created_utc": 1600000000.0, "subreddit": "Nootropics", "metadata": {}}
            ]
            
            generator = service.collect_data(params)
            results = list(generator)
            
            assert len(results) >= 2
            assert hasattr(results[-1], 'collection_hash')
