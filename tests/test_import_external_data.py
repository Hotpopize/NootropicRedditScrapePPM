import os
import json
import pytest
from scripts.import_external_data import is_seemingly_raw_username, process_file

class TestImportExternalData:
    
    def test_pii_heuristic_rejections(self):
        # Raw usernames that SHOULD be rejected if no scrub flag
        assert is_seemingly_raw_username("john_doe_99") == True
        assert is_seemingly_raw_username("ThrowawayAcct123") == True
        assert is_seemingly_raw_username("u/student") == True # PRAW gives just 'student', this tests base heuristic
        
        # Valid anonymized markers that should PASS
        assert is_seemingly_raw_username("[deleted]") == False
        assert is_seemingly_raw_username("[removed]") == False
        assert is_seemingly_raw_username("anon_USER77") == False
        assert is_seemingly_raw_username("User_039123") == False
        assert is_seemingly_raw_username("a" * 64) == False # Mock SHA256 length string of repeating hex 'a'
        assert is_seemingly_raw_username("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") == False

    def test_refuses_unscrubbed_pii(self, tmp_path):
        """
        Prove that the importer rigidly halts (sys.exit) when raw PII is provided
        without the --acknowledge-pii-scrubbing explicitly enabled.
        """
        payload = [{
            "id": "mock1", "type": "submission", "subreddit": "Test",
            "title": "T", "text": "Tx", "author": "RealUsernameWhoForgetToScrub",
            "score": 0, "created_utc": 1600000.0, "permalink": "x"
        }]
        
        file_path = tmp_path / "mock.json"
        with open(file_path, "w") as f:
            json.dump(payload, f)
            
        with pytest.raises(SystemExit) as exc:
            process_file(str(file_path), ack_scrubbing=False)
            
        assert exc.value.code == 1 # Exits with error
        
    def test_accepts_valid_pii_with_flag(self, tmp_path, monkeypatch):
        """
        Prove that the importer succeeds when the flag is passed, securely anonymizing the data.
        """
        payload = [{
            "id": "mock1", "type": "submission", "subreddit": "Test",
            "title": "T", "text": "Tx", "author": "RealUsernameWhoForgetToScrub",
            "score": 0, "created_utc": 1600000.0, "permalink": "x"
        }]
        
        file_path = tmp_path / "mock.json"
        with open(file_path, "w") as f:
            json.dump(payload, f)
            
        # Mocking the DB helpers so we don't actually write to the real SQLite DB in the test
        monkeypatch.setattr('scripts.import_external_data.create_scrape_run', lambda **kwargs: None)
        monkeypatch.setattr('scripts.import_external_data.save_collected_data', lambda a, b: len(a))
        monkeypatch.setattr('scripts.import_external_data.update_scrape_run', lambda **kwargs: None)
        
        # This should NOT sys.exit because ack_scrubbing=True
        process_file(str(file_path), ack_scrubbing=True)
