# scripts/import_external_data.py
# ================================
# Compliance: Reddit Research Data Addendum (executed 2026-04-01)
# ---------------------------------------------------------------------------
# This module implements the external-data ingestion gate with enforced 
# PII constraints (§2.b). See COMPLIANCE.md for the full mapping.
#
import argparse
import csv
import json
import logging
import os
import sys
import uuid
import hashlib
from datetime import datetime

from pydantic import ValidationError

# Ensure we can import from core/utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.schemas import CollectedItem
from utils.db_helpers import save_collected_data, create_scrape_run, update_scrape_run
from utils.anonymize_data import DataAnonymizer

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def is_seemingly_raw_username(username: str) -> bool:
    """
    Heuristically checks if a string looks like a raw username rather than a
    pseudonymized hash or [deleted]. Rejects strings that do not look safe.
    """
    if not username:
        return False
        
    lower_user = username.strip().lower()
    
    # Safe known tags
    if lower_user in ["[deleted]", "[removed]"]:
        return False
        
    # Standard anonymization script pseudonym format (e.g. User_012345 or User_001)
    if lower_user.startswith("user_"):
        return False
        
    # Explicit anon prefix
    if lower_user.startswith("anon_") or lower_user.startswith("pseudonym_"):
        return False
        
    # SHA256 hashed (64 characters of hex)
    if len(lower_user) == 64 and all(c in "0123456789abcdef" for c in lower_user):
        return False
        
    # If not matching any safe heuristic, we assume it's raw PII
    return True

def parse_csv_row(row: dict) -> dict:
    """
    Molds a flat CSV row into the nested dictionary structure expected by our
    schema. Specifically handles reconstructing the `metadata` dict from prefixed
    columns or populates sensible defaults.
    """
    item = dict(row)
    
    # Cast integers & floats where needed
    try:
        item['score'] = int(item.get('score', 0))
    except (ValueError, TypeError):
        item['score'] = 0
        
    try:
        item['created_utc'] = float(item.get('created_utc', datetime.utcnow().timestamp()))
    except (ValueError, TypeError):
        item['created_utc'] = datetime.utcnow().timestamp()

    # Reconstruct nested metadata if columns were flat (e.g., 'metadata.nsfw')
    metadata = {}
    keys_to_delete = []
    
    for key, value in item.items():
        if key.startswith('metadata.'):
            sub_key = key.split('metadata.', 1)[1]
            
            # Basic parsing mapping for known booleans
            if value.lower() in ['true', '1', 'yes']:
                parsed_val = True
            elif value.lower() in ['false', '0', 'no']:
                parsed_val = False
            elif sub_key in ['text_length', 'word_count', 'depth']:
                try: parsed_val = int(value)
                except ValueError: parsed_val = None
            else:
                parsed_val = value if value != "" else None
                
            metadata[sub_key] = parsed_val
            keys_to_delete.append(key)
            
    for k in keys_to_delete:
        del item[k]
        
    item['metadata'] = metadata
    
    # Fill defaults if metadata doesn't exist
    if not item['metadata']:
        item['metadata'] = {
            'content_status': 'available',
            'language_flag': 'not_checked',
            'text_length': len(item.get('text', '')),
            'data_source': 'external_import'
        }
        
    return item

def process_file(file_path: str, ack_scrubbing: bool = False):
    """
    Validates, anonymizes (if approved), and saves records to SQLite DB.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
        
    data = []
    ext = os.path.splitext(file_path)[1].lower()
    
    # LOAD
    if ext == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            if not isinstance(raw_data, list):
                logger.error("JSON payload must be an array of objects.")
                sys.exit(1)
            data = raw_data
            
    elif ext == '.csv':
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = [parse_csv_row(row) for row in reader]
    else:
        logger.error("Unsupported file extension. Only .csv or .json are allowed.")
        sys.exit(1)
        
    logger.info(f"Loaded {len(data)} records from {file_path}. Processing...")
    
    # PII CHECK & ANONYMIZE
    anonymizer = DataAnonymizer()
    rejected = 0
    anonymized = 0
    safe_data = []
    
    for idx, item in enumerate(data):
        author = item.get('author', '')
        
        if is_seemingly_raw_username(author):
            if not ack_scrubbing:
                rejected += 1
            else:
                # Use deterministic SHA256 to pass strict schema validation
                item['author'] = hashlib.sha256(author.encode()).hexdigest()
                anonymized += 1
                
        safe_data.append(item)
        
    if rejected > 0:
        logger.error(f"Halted! Found {rejected} raw/un-hashed usernames.")
        logger.error("You MUST explicitly flag `--acknowledge-pii-scrubbing` to allow "
                     "the importer to securely anonymize these usernames automatically.")
        sys.exit(1)
        
    if anonymized > 0:
        logger.info(f"Securely anonymized {anonymized} usernames.")
        
    # SCHEMA VALIDATION (Pydantic V2)
    validated_items = []
    for idx, item in enumerate(safe_data):
        # Force the collected_at and source
        item['collected_at'] = datetime.utcnow().isoformat()
        item['data_source'] = 'external_import'
        item.setdefault('metadata', {})['data_source'] = 'external_import'
        
        try:
            # We strictly enforce the CollectedItem schema to guarantee downstream pipeline safety
            validated_item = CollectedItem.model_validate(item).model_dump()
            validated_items.append(validated_item)
        except ValidationError as e:
            logger.error(f"Row {idx} failed schema validation: {e}")
            sys.exit(1)

    # DATABASE INGESTION
    session_id = datetime.now().strftime("%Y%m%d_%H%M%S_ext")
    job_id = str(uuid.uuid4())
    
    # Create fake run so it shows in the UI dashboard accurately
    create_scrape_run(
        job_id=job_id,
        config_hash="EXTERNAL_IMPORT_HASH",
        parameters={"source": file_path, "data_source": "external_import"},
        session_id=session_id,
        label=f"External Import: {os.path.basename(file_path)}"
    )
    
    saved_count = save_collected_data(validated_items, session_id)
    update_scrape_run(job_id=job_id, status='COMPLETED', items_collected=saved_count)
    
    logger.info(f"Success! {saved_count} records imported into session: {session_id}")
    logger.info("These records are now available in the Web App's Coding pipeline.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import external BYO-data datasets into the Nootropic analysis tool.")
    parser.add_argument("input_file", help="Path to the JSON or CSV file containing Reddit-like records.")
    parser.add_argument("--acknowledge-pii-scrubbing", action="store_true", 
                        help="Opt-in to securely anonymize raw usernames actively upon import.")
    args = parser.parse_args()
    
    try:
        process_file(args.input_file, args.acknowledge_pii_scrubbing)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)
