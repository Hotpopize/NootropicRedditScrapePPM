import argparse
import logging
import prawcore
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from services.reddit_service import RedditService
from utils.db_helpers import get_all_collected_reddit_ids, delete_collected_data_by_ids, log_action
from core.schemas import RedditCredentials

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 100  # Reddit API limit for info()

def main():
    parser = argparse.ArgumentParser(description="Scrub deleted Reddit data from the local database for compliance.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted without actually deleting")
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "python:NootropicRedditScrapePPM:v1.0 (by /u/unknown)")
    
    if not all([client_id, client_secret, user_agent]):
        logger.error("Missing Reddit credentials in .env file.")
        sys.exit(1)

    credentials = RedditCredentials(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    
    logger.info("Initializing Reddit service...")
    try:
        reddit_service = RedditService(credentials=credentials)
        if not reddit_service.verify_credentials():
            logger.error("Failed to verify Reddit credentials.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error initializing Reddit service: {e}")
        sys.exit(1)

    logger.info("Fetching items from database for compliance check...")
    fullnames = get_all_collected_reddit_ids()
    
    if not fullnames:
        logger.info("No items found in the database. Nothing to scrub.")
        sys.exit(0)

    logger.info(f"Found {len(fullnames)} items to check.")

    # Create fullname mapping
    fullname_to_raw_id = {fn: fn[3:] for fn in fullnames}

    total_checked = 0
    total_deleted = 0
    all_deleted_raw_ids = []

    # Process in batches
    for i in range(0, len(fullnames), BATCH_SIZE):
        batch_fullnames = fullnames[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(fullnames) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_fullnames)} items)...")
        
        try:
            # Yields only the objects that still exist AND we have permission to view
            returned_items = list(reddit_service.reddit.info(fullnames=batch_fullnames))
            
            # Map returned fullnames (what Reddit says still exists)
            returned_fullnames = set([item.name for item in returned_items])
            
            # 1. HARD DELETIONS: Requested minus Returned
            hard_deleted_fullnames = set(batch_fullnames) - returned_fullnames
            
            # 2. SOFT DELETIONS/REMOVALS: Check returned content
            soft_deleted_fullnames = set()
            for item in returned_items:
                author_name = str(item.author) if item.author else '[deleted]'
                # PRAW Submission uses 'selftext', Comment uses 'body'
                text_content = getattr(item, 'selftext', '') if hasattr(item, 'selftext') else getattr(item, 'body', '')
                
                if author_name == '[deleted]' or text_content in ['[removed]', '[deleted]']:
                    soft_deleted_fullnames.add(item.name)
            
            # Combine
            deleted_fullnames = hard_deleted_fullnames.union(soft_deleted_fullnames)
            
            if deleted_fullnames:
                deleted_raw_ids = [fullname_to_raw_id[fn] for fn in deleted_fullnames]
                all_deleted_raw_ids.extend(deleted_raw_ids)
                
                logger.info(f"Batch {batch_num}/{total_batches}: Found {len(deleted_raw_ids)} items to delete (Hard: {len(hard_deleted_fullnames)}, Soft: {len(soft_deleted_fullnames)}).")
                
                if args.dry_run:
                    logger.info(f"[DRY-RUN] Would delete raw IDs: {deleted_raw_ids}")
                else:
                    logger.info("Writing audit log and deleting from database...")
                    log_action("COMPLIANCE_PURGE", "SYSTEM", {"deleted_ids": deleted_raw_ids})
                    count = delete_collected_data_by_ids(deleted_raw_ids)
                    total_deleted += count
            else:
                logger.debug(f"Batch {batch_num}/{total_batches}: No deleted items found.")
                
            total_checked += len(batch_fullnames)

        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException) as e:
            logger.error(f"API Error on batch {batch_num}/{total_batches}, skipping batch. Error: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error on batch {batch_num}/{total_batches}: {e}")
            continue

    # Final summary
    logger.info("=== Scrubbing Complete ===")
    logger.info(f"Total items checked:  {total_checked}")
    if args.dry_run:
        logger.info(f"Total items that WOULD be deleted: {len(all_deleted_raw_ids)}")
    else:
        logger.info(f"Total items actually deleted:      {total_deleted}")

if __name__ == "__main__":
    main()
