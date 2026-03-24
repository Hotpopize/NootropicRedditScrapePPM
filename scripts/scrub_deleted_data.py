# scripts/scrub_deleted_data.py
# ==============================
# Compliance scrubber for the NootropicRedditScrapePPM thesis tool.
#
# Purpose
# -------
# Checks every collected Reddit submission against the live Reddit API and
# purges any item that has been hard-deleted, soft-deleted, or removed by a
# moderator.  This keeps the local research database consistent with the
# platform's content-removal decisions — a GDPR-adjacent ethical requirement
# documented in thesis methodology Chapter 3.
#
# When to run
# -----------
# Run manually before any data-analysis phase and after any bulk collection
# session.  The script is NOT invoked by the Streamlit app itself.
#
#   python scripts/scrub_deleted_data.py             # live run
#   python scripts/scrub_deleted_data.py --dry-run   # preview only
#   python scripts/scrub_deleted_data.py --check-credentials
#
# Credentials requirement and known limitation
# --------------------------------------------
# Full compliance scrubbing requires Reddit API credentials (PRAW script-type
# app).  If the tool is running in JSON-only mode (no .env credentials), this
# script exits cleanly with code 0 after logging the limitation message.
# The Reddit public JSON endpoint cannot replicate reddit.info(fullnames=...)
# — there is no batch-lookup equivalent without authentication.
# This limitation is documented in thesis methodology Chapter 3.
#
# Exported symbols: none (standalone script only)

import argparse
import logging
import os
import sys

import prawcore

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from core.schemas import RedditCredentials
from services.reddit_service import RedditService
from utils.db_helpers import (
    delete_collected_data_by_ids,
    get_all_collected_reddit_ids,
    log_action,
)

# ---------------------------------------------------------------------------
# Logging — standalone basicConfig (not inherited from app.py in script mode)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Reddit API hard limit for the info() fullnames parameter.
BATCH_SIZE = 100

# Message printed when credentials are absent.  Wording is fixed so it can be
# cited verbatim in the thesis methodology chapter.
_NO_CREDENTIALS_MSG = (
    "Compliance scrubbing requires Reddit API credentials. "
    "If operating in JSON-only mode, manual review of deleted content is required. "
    "See thesis methodology Chapter 3 for the documented limitation."
)


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------

def _load_credentials() -> tuple[str | None, str | None, str]:
    """
    Load Reddit API credentials from the environment (.env file).

    Returns (client_id, client_secret, user_agent).
    client_id and client_secret may be None if not configured.
    user_agent always has a non-empty default value.
    """
    load_dotenv()
    client_id     = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent    = os.getenv(
        "REDDIT_USER_AGENT",
        "python:NootropicRedditScrapePPM:v1.0 (by /u/unknown)",
    )
    return client_id, client_secret, user_agent


def _credentials_present(client_id: str | None, client_secret: str | None) -> bool:
    """Return True only if both required OAuth fields are non-empty."""
    return bool(client_id and client_secret)


# ---------------------------------------------------------------------------
# --check-credentials handler
# ---------------------------------------------------------------------------

def run_credential_check() -> None:
    """
    Verify whether Reddit API credentials are configured and valid.
    Exits with code 0 regardless of outcome — this is a diagnostic, not an error.
    """
    client_id, client_secret, user_agent = _load_credentials()

    if not _credentials_present(client_id, client_secret):
        logger.warning("Credential check: no credentials found in environment.")
        logger.warning(_NO_CREDENTIALS_MSG)
        sys.exit(0)

    logger.info("Credential check: credentials found, verifying with Reddit API...")
    try:
        credentials   = RedditCredentials(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        reddit_service = RedditService(credentials=credentials)
        if reddit_service.verify_credentials():
            logger.info("Credential check: PASSED — Reddit API credentials are valid.")
        else:
            logger.error("Credential check: FAILED — credentials were rejected by Reddit.")
    except Exception as exc:
        logger.error("Credential check: ERROR — %s", exc)

    sys.exit(0)


# ---------------------------------------------------------------------------
# Main scrub logic
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrub deleted Reddit data from the local database for compliance. "
            "Requires Reddit API credentials unless --check-credentials is used."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without actually deleting.",
    )
    parser.add_argument(
        "--check-credentials",
        action="store_true",
        help=(
            "Verify whether Reddit API credentials are configured and valid, "
            "then exit without running the scrub."
        ),
    )
    args = parser.parse_args()

    # Diagnostic mode — never runs the scrub.
    if args.check_credentials:
        run_credential_check()
        # run_credential_check() always calls sys.exit — unreachable below.

    # -----------------------------------------------------------------------
    # Credential gate — graceful fallback if not configured
    # -----------------------------------------------------------------------
    client_id, client_secret, user_agent = _load_credentials()

    if not _credentials_present(client_id, client_secret):
        logger.warning("Compliance scrub skipped: Reddit API credentials are not configured.")
        logger.warning(_NO_CREDENTIALS_MSG)
        sys.exit(0)

    # -----------------------------------------------------------------------
    # Credential verification — hard error if present but invalid
    # -----------------------------------------------------------------------
    credentials = RedditCredentials(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    logger.info("Initialising Reddit service...")
    try:
        reddit_service = RedditService(credentials=credentials)
        if not reddit_service.verify_credentials():
            logger.error("Failed to verify Reddit credentials.")
            sys.exit(1)
    except Exception as exc:
        logger.error("Error initialising Reddit service: %s", exc)
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Fetch items to check
    # -----------------------------------------------------------------------
    logger.info("Fetching items from database for compliance check...")
    fullnames = get_all_collected_reddit_ids()

    if not fullnames:
        logger.info("No items found in the database. Nothing to scrub.")
        sys.exit(0)

    logger.info("Found %d items to check.", len(fullnames))

    # Pre-build reverse mapping: t3_abc123 → abc123 (bare DB id)
    fullname_to_raw_id = {fn: fn[3:] for fn in fullnames}

    total_checked     = 0
    total_deleted     = 0
    all_deleted_raw_ids: list[str] = []

    # -----------------------------------------------------------------------
    # Batch processing
    # -----------------------------------------------------------------------
    total_batches = (len(fullnames) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(fullnames), BATCH_SIZE):
        batch_fullnames = fullnames[i : i + BATCH_SIZE]
        batch_num       = (i // BATCH_SIZE) + 1

        logger.info(
            "Processing batch %d/%d (%d items)...",
            batch_num, total_batches, len(batch_fullnames),
        )

        try:
            # reddit.info() yields only items that still exist AND are visible.
            returned_items = list(reddit_service.reddit.info(fullnames=batch_fullnames))

            returned_fullnames = {item.name for item in returned_items}

            # Hard deletions: requested fullnames that Reddit did not return at all.
            hard_deleted_fullnames = set(batch_fullnames) - returned_fullnames

            # Soft deletions / mod removals: returned but content is gone.
            soft_deleted_fullnames: set[str] = set()
            for item in returned_items:
                author_name  = str(item.author) if item.author else "[deleted]"
                # Submissions have selftext; comments have body.
                text_content = (
                    getattr(item, "selftext", "")
                    if hasattr(item, "selftext")
                    else getattr(item, "body", "")
                )
                if author_name == "[deleted]" or text_content in ("[removed]", "[deleted]"):
                    soft_deleted_fullnames.add(item.name)

            deleted_fullnames = hard_deleted_fullnames | soft_deleted_fullnames

            if deleted_fullnames:
                deleted_raw_ids = [fullname_to_raw_id[fn] for fn in deleted_fullnames]
                all_deleted_raw_ids.extend(deleted_raw_ids)

                logger.info(
                    "Batch %d/%d: %d items to delete (hard: %d, soft: %d).",
                    batch_num, total_batches,
                    len(deleted_raw_ids),
                    len(hard_deleted_fullnames),
                    len(soft_deleted_fullnames),
                )

                if args.dry_run:
                    logger.info("[DRY-RUN] Would delete raw IDs: %s", deleted_raw_ids)
                else:
                    logger.info("Writing audit log and deleting from database...")
                    # session_id is "SYSTEM" here — this script runs outside any
                    # Streamlit session, so there is no user session ID to reference.
                    log_action("COMPLIANCE_PURGE", "SYSTEM", {"deleted_ids": deleted_raw_ids})
                    count          = delete_collected_data_by_ids(deleted_raw_ids)
                    total_deleted += count
            else:
                logger.debug(
                    "Batch %d/%d: no deleted items found.",
                    batch_num, total_batches,
                )

            total_checked += len(batch_fullnames)

        except (prawcore.exceptions.ServerError, prawcore.exceptions.ResponseException) as exc:
            logger.error(
                "API error on batch %d/%d, skipping. Error: %s",
                batch_num, total_batches, exc,
            )
            continue
        except Exception as exc:
            logger.error(
                "Unexpected error on batch %d/%d: %s",
                batch_num, total_batches, exc,
            )
            continue

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    logger.info("=== Scrubbing Complete ===")
    logger.info("Total items checked:  %d", total_checked)
    if args.dry_run:
        logger.info("Total items that WOULD be deleted: %d", len(all_deleted_raw_ids))
    else:
        logger.info("Total items actually deleted:      %d", total_deleted)


if __name__ == "__main__":
    main()
