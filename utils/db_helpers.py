"""
utils/db_helpers.py
===================
Database access layer for NootropicRedditScrapePPM.

All functions open and close their own session — no shared session state.
Session management pattern: get_db_session() → try/finally → db.close()

Function inventory by table
----------------------------
collected_data:
  save_collected_data(items, session_id) → int
  load_collected_data(session_id, subreddit, limit) → List[dict]
  get_all_collected_reddit_ids() → List[str]      # t3_-prefixed fullnames
  delete_collected_data_by_ids(reddit_ids) → int  # accepts bare OR t3_-prefixed IDs

coded_data:
  save_coded_data(items, session_id) → int
  load_coded_data(session_id, limit) → List[dict]

codebook:
  save_codebook(codebook_data, session_id)
  load_codebook(session_id) → dict

scrape_runs:
  create_scrape_run(job_id, config_hash, parameters, session_id)
  update_scrape_run(job_id, status, items_collected, error_message)

audit_log:
  log_action(action, session_id, details)
  load_audit_logs(session_id, action_filter, limit) → List[dict]

replicability_log:
  save_replicability_log(collection_hash, session_id, parameters, statistics, ...)
  load_replicability_logs(session_id, limit) → List[dict]

reporting:
  get_data_quality_report() → dict

zotero (inactive — future use):
  save_zotero_references, load_zotero_references, get_all_zotero_keywords
  save_citation_links, load_citation_links

Callers
-------
app.py                  — load_collected_data, load_coded_data, load_codebook
modules/reddit_scraper  — save_collected_data, log_action, save_replicability_log,
                          get_all_zotero_keywords, load_collected_data (post-job)
modules/llm_coder       — save_coded_data, log_action, load_codebook
modules/codebook        — save_codebook, load_codebook
modules/data_manager    — load_audit_logs, log_action
modules/thesis_export   — load_replicability_logs, get_data_quality_report,
                          load_citation_links, load_zotero_references
modules/topic_modeling  — log_action
modules/reliability     — log_action
services/reddit_service — save_collected_data, update_scrape_run
services/job_manager    — create_scrape_run, update_scrape_run, log_action
scripts/scrub_deleted_data (planned) — get_all_collected_reddit_ids,
                                       delete_collected_data_by_ids
"""

import logging
from datetime import datetime

from core.database import (
    get_db_session,
    CollectedData, CodedData, Codebook,
    AuditLog, ReplicabilityLog,
    ZoteroReference, ZoteroCollectionLink,
    ScrapeRun,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ScrapeRun — job lifecycle tracking
# ---------------------------------------------------------------------------

def create_scrape_run(job_id: str, config_hash: str,
                      parameters: dict, session_id: str = None) -> None:
    """Create a new ScrapeRun record at job start. Status is set to RUNNING."""
    db = get_db_session()
    try:
        run = ScrapeRun(
            job_id=job_id,
            status='RUNNING',
            config_hash=config_hash,
            parameters=parameters,
            session_id=session_id,
        )
        db.add(run)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def update_scrape_run(job_id: str, status: str = None,
                      items_collected: int = None,
                      error_message: str = None) -> None:
    """
    Update an existing ScrapeRun record.
    Called by:
      - services/reddit_service.py (incremental items_collected updates)
      - services/job_manager.py (status transitions: COMPLETED / FAILED / CANCELLED)

    NOTE: status='' evaluates as falsy — pass None to skip status update,
    not an empty string.
    """
    db = get_db_session()
    try:
        run = db.query(ScrapeRun).filter_by(job_id=job_id).first()
        if run:
            if status:
                run.status = status
                if status in ('COMPLETED', 'FAILED', 'CANCELLED'):
                    run.completed_at = datetime.utcnow()
            if items_collected is not None:
                run.items_collected = items_collected
            if error_message:
                run.error_message = error_message
            db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CollectedData — raw Reddit posts / comments
# ---------------------------------------------------------------------------

def save_collected_data(items: list, session_id: str) -> int:
    """
    Persist a batch of collected item dicts to collected_data.

    Deduplication: application-level check on reddit_id before each insert.
    The DB also has a UNIQUE constraint on reddit_id as a second safety net.

    Expected item dict keys (see RedditItem in core/schemas.py):
      id, type, subreddit, title, text, author, score, created_utc,
      num_comments, url, permalink, post_id, data_source, metadata

    data_source defaults to 'praw' if not present in the item dict.
    Returns the count of newly inserted records (skipped duplicates not counted).

    NOTE: Called from the background thread in reddit_service.py in 50-item
    batches. The buffer is cleared after each batch so CollectionResult ends
    up with an empty collected_posts list — this is expected behaviour.
    Do not call save_collected_data(final_result.collected_posts) in the UI.
    """
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(CollectedData).filter_by(
                reddit_id=item.get('id')
            ).first()
            if not existing:
                record = CollectedData(
                    reddit_id      = item.get('id'),
                    type           = item.get('type'),
                    subreddit      = item.get('subreddit'),
                    title          = item.get('title'),
                    text           = item.get('text'),
                    author         = item.get('author'),
                    score          = item.get('score', 0),
                    created_utc    = item.get('created_utc'),
                    num_comments   = item.get('num_comments'),
                    url            = item.get('url'),
                    permalink      = item.get('permalink'),
                    post_id        = item.get('post_id'),
                    session_id     = session_id,
                    data_source    = item.get('data_source', 'praw'),
                    extra_metadata = item.get('metadata', {}),
                )
                db.add(record)
                saved_count += 1

        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_collected_data(session_id: str = None,
                        subreddit: str = None,
                        limit: int = None) -> list:
    """
    Load collected item dicts from the database.

    session_id=None loads all records across all sessions (used by app.py
    startup and post-job refresh in reddit_scraper.py).

    limit=10000 is applied by callers — records beyond this cap are silently
    excluded. Acceptable for a 150–200 post thesis dataset.

    data_source coalesces NULL → 'praw' for rows predating the migration.

    Returns list of dicts matching the RedditItem schema shape.
    """
    db = get_db_session()
    try:
        query = db.query(CollectedData)

        if session_id:
            query = query.filter_by(session_id=session_id)
        if subreddit:
            query = query.filter_by(subreddit=subreddit)

        query = query.order_by(CollectedData.collected_at.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        return [
            {
                'id':           r.reddit_id,
                'type':         r.type,
                'subreddit':    r.subreddit,
                'title':        r.title,
                'text':         r.text,
                'author':       r.author,
                'score':        r.score,
                'created_utc':  r.created_utc,
                'num_comments': r.num_comments,
                'url':          r.url,
                'permalink':    r.permalink,
                'post_id':      r.post_id,
                'data_source':  r.data_source or 'praw',
                'collected_at': r.collected_at.isoformat() if r.collected_at else None,
                'metadata':     r.extra_metadata or {},
            }
            for r in results
        ]
    finally:
        db.close()


def get_all_collected_reddit_ids() -> list:
    """
    Return t3_-prefixed fullnames for all collected submissions.

    Used by scrub_deleted_data.py to pass to reddit.info(fullnames=[...]).
    Only submissions are included (t3_ prefix) — comments would need t1_.

    IMPORTANT for scrub_deleted_data.py:
      The returned fullnames are for the Reddit API call only.
      When calling delete_collected_data_by_ids(), strip the 't3_' prefix
      first — the DB stores bare IDs (e.g. 'abc123', not 't3_abc123').
      delete_collected_data_by_ids() will strip the prefix defensively,
      but explicit stripping in the caller makes intent clear.
    """
    db = get_db_session()
    try:
        results = (
            db.query(CollectedData)
            .filter_by(type='submission')
            .all()
        )
        return ['t3_' + r.reddit_id for r in results]
    finally:
        db.close()


def delete_collected_data_by_ids(reddit_ids: list) -> int:
    """
    Permanently delete CollectedData rows by reddit_id.

    Accepts EITHER bare IDs ('abc123') OR t3_-prefixed fullnames ('t3_abc123').
    The t3_ prefix is stripped defensively on entry so callers don't need to
    normalise before calling.

    Returns the count of deleted rows.

    WARNING: Destructive and unrecoverable. Used exclusively by the compliance
    scrubber (scrub_deleted_data.py) to remove posts that have been deleted
    from Reddit. Always run the scrubber with --dry-run first.
    """
    # Normalise: strip t3_ prefix if present
    bare_ids = [
        rid[3:] if rid.startswith('t3_') else rid
        for rid in reddit_ids
    ]

    if not bare_ids:
        return 0

    db = get_db_session()
    try:
        deleted_count = (
            db.query(CollectedData)
            .filter(CollectedData.reddit_id.in_(bare_ids))
            .delete(synchronize_session=False)
        )
        db.commit()
        return deleted_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CodedData — LLM PPM coding results
# ---------------------------------------------------------------------------

def save_coded_data(items: list, session_id: str) -> int:
    """
    Upsert coded item dicts into coded_data.

    If a record with the same reddit_id already exists, it is overwritten
    (re-coding replaces previous coding). New records are inserted.

    Returns the total count of records processed (inserts + updates).
    """
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(CodedData).filter_by(
                reddit_id=item.get('id')
            ).first()

            if existing:
                existing.ppm_category    = item.get('ppm_category')
                existing.ppm_subcodes    = item.get('ppm_subcodes', [])
                existing.themes          = item.get('themes', [])
                existing.evidence_quotes = item.get('evidence_quotes', [])
                existing.confidence      = item.get('confidence')
                existing.coded_at        = datetime.utcnow()
                existing.coded_by        = item.get('coded_by')
                existing.coding_approach = item.get('coding_approach')
                existing.rationale       = item.get('rationale')
                existing.extra_metadata  = item.get('metadata', {})
                existing.session_id      = session_id
            else:
                record = CodedData(
                    reddit_id       = item.get('id'),
                    ppm_category    = item.get('ppm_category'),
                    ppm_subcodes    = item.get('ppm_subcodes', []),
                    themes          = item.get('themes', []),
                    evidence_quotes = item.get('evidence_quotes', []),
                    confidence      = item.get('confidence'),
                    coded_by        = item.get('coded_by'),
                    coding_approach = item.get('coding_approach'),
                    session_id      = session_id,
                    rationale       = item.get('rationale'),
                    extra_metadata  = item.get('metadata', {}),
                )
                db.add(record)

            saved_count += 1

        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_coded_data(session_id: str = None, limit: int = None) -> list:
    """Load coded item dicts. session_id=None returns all sessions."""
    db = get_db_session()
    try:
        query = db.query(CodedData)

        if session_id:
            query = query.filter_by(session_id=session_id)

        query = query.order_by(CodedData.coded_at.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        return [
            {
                'id':              r.reddit_id,
                'ppm_category':    r.ppm_category,
                'ppm_subcodes':    r.ppm_subcodes or [],
                'themes':          r.themes or [],
                'evidence_quotes': r.evidence_quotes or [],
                'confidence':      r.confidence,
                'coded_at':        r.coded_at.isoformat() if r.coded_at else None,
                'coded_by':        r.coded_by,
                'coding_approach': r.coding_approach,
                'rationale':       r.rationale,
                'metadata':        r.extra_metadata or {},
            }
            for r in results
        ]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Codebook — PPM code definitions
# ---------------------------------------------------------------------------

def save_codebook(codebook_data: dict, session_id: str) -> None:
    """
    Upsert codebook codes and prune removed ones.

    IMPORTANT — session-scoped prune:
    The prune step deletes codes in the CURRENT SESSION that are not in
    codebook_data. It does NOT touch codes from other sessions.
    This prevents a new session's save from wiping codes saved in a previous
    session (which the original global prune did).

    If a (category, name) pair already exists in a different session, it is
    updated (session_id reassigned) rather than duplicated. This is correct
    for a single-researcher tool where all codes represent the same codebook.
    """
    db = get_db_session()
    try:
        current_codes = set()
        codes_list = (
            codebook_data.get('codes', [])
            if isinstance(codebook_data, dict)
            else []
        )

        for code in codes_list:
            category = code.get('category')
            name     = code.get('name')
            current_codes.add((category, name))

            existing = db.query(Codebook).filter_by(
                category=category,
                name=name,
            ).first()

            meta = {
                'id':                   code.get('id'),
                'include':              code.get('include'),
                'exclude':              code.get('exclude'),
                'source':               code.get('source'),
                'is_emergent_candidate': code.get('is_emergent_candidate'),
                'created_at':           code.get('created_at'),
            }

            if existing:
                existing.definition    = code.get('definition')
                existing.examples      = code.get('examples')
                existing.frequency     = code.get('frequency', 0)
                existing.extra_metadata = meta
                existing.session_id    = session_id
            else:
                record = Codebook(
                    category       = category,
                    name           = name,
                    definition     = code.get('definition'),
                    examples       = code.get('examples'),
                    frequency      = code.get('frequency', 0),
                    session_id     = session_id,
                    extra_metadata = meta,
                )
                db.add(record)

        # FIX: prune scoped to current session only (was global — data loss bug)
        session_codes = (
            db.query(Codebook)
            .filter_by(session_id=session_id)
            .all()
        )
        for db_code in session_codes:
            if (db_code.category, db_code.name) not in current_codes:
                db.delete(db_code)

        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_codebook(session_id: str = None) -> dict:
    """
    Load codebook codes as a dict with shape {'codes': [...]}.

    session_id=None returns all codes across all sessions (used by app.py
    startup). When multiple sessions have conflicting codes for the same
    (category, name) pair, the first encountered wins (seen_codes dedup).

    Called by:
      - app.py (session_id=None — load all)
      - modules/codebook.py (session_id from session state)
      - modules/llm_coder.py (session_id from session state)
    """
    db = get_db_session()
    try:
        if session_id:
            results = db.query(Codebook).filter_by(session_id=session_id).all()
        else:
            results = db.query(Codebook).all()

        codes_list = []
        seen_codes: dict = {}

        for r in results:
            key = (r.category, r.name)
            if key in seen_codes:
                continue
            seen_codes[key] = True

            meta = r.extra_metadata or {}
            codes_list.append({
                'id':                   meta.get('id') or f"{r.category}-{r.name}",
                'category':             r.category,
                'name':                 r.name,
                'definition':           r.definition,
                'examples':             r.examples,
                'frequency':            r.frequency,
                'include':              meta.get('include', ''),
                'exclude':              meta.get('exclude', ''),
                'source':               meta.get('source', ''),
                'is_emergent_candidate': meta.get('is_emergent_candidate', False),
                'created_at':           meta.get('created_at') or (
                    r.added_at.isoformat() if r.added_at else None
                ),
            })

        return {'codes': codes_list}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# AuditLog — discrete action logging
# ---------------------------------------------------------------------------

def log_action(action: str, session_id: str, details: dict) -> None:
    """
    Write a single action record to audit_log.

    Called by: job_manager (job lifecycle), reddit_scraper (collection complete),
    llm_coder (coding complete), data_manager (export), topic_modeling,
    reliability modules.

    Known action values: 'job_completed', 'job_crash', 'job_cancelled',
    'job_init_failed', 'data_collection', 'automated_coding_ollama', 'export'
    """
    db = get_db_session()
    try:
        log = AuditLog(
            action     = action,
            session_id = session_id,
            details    = details,
        )
        db.add(log)
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_audit_logs(session_id: str = None,
                    action_filter: str = None,
                    limit: int = 100) -> list:
    """
    Load audit log records for display in the Data Export module.

    session_id=None returns logs across all sessions (fallback when
    session_id is unavailable). Passing an empty string behaves the same.

    action_filter: optional string to filter to a specific action type.

    NOTE: data_manager.py previously read audit logs from a flat JSONL file.
    It now calls this function directly from the DB. The file-based approach
    is removed.
    """
    db = get_db_session()
    try:
        query = db.query(AuditLog)

        # Guard: treat empty string same as None
        if session_id:
            query = query.filter_by(session_id=session_id)

        if action_filter:
            query = query.filter_by(action=action_filter)

        query = query.order_by(AuditLog.timestamp.desc()).limit(limit)

        results = query.all()

        return [
            {
                'timestamp':  r.timestamp.isoformat() if r.timestamp else None,
                'action':     r.action,
                'session_id': r.session_id,
                'details':    r.details or {},
            }
            for r in results
        ]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# ReplicabilityLog — authoritative methodology audit trail
# ---------------------------------------------------------------------------

def save_replicability_log(
    collection_hash:   str,
    session_id:        str,
    parameters:        dict,
    statistics:        dict,
    rate_limit_events: list  = None,
    validation_results: dict = None,
    notes:             str   = None,
) -> None:
    """
    Upsert a replicability log record keyed on collection_hash.

    Upsert behaviour: if the same collection_hash already exists (same
    CollectionParams.model_dump() → same hash), the existing record is
    overwritten. Two runs with identical parameters in the same session
    produce the same hash and the second run's stats replace the first.
    This is intentional — the hash represents a unique parameter set,
    not a unique run timestamp.

    parameters dict MUST include 'data_source' key ('praw' | 'json_endpoint')
    for full methodology traceability. This is set by modules/reddit_scraper.py
    using st.session_state.active_data_source.

    This table is the authoritative source for Chapter 3 methodology
    documentation — it records exactly what was collected, with what
    parameters, via which collection method.
    """
    db = get_db_session()
    try:
        existing = db.query(ReplicabilityLog).filter_by(
            collection_hash=collection_hash
        ).first()

        if existing:
            existing.parameters        = parameters
            existing.statistics        = statistics
            existing.rate_limit_events = rate_limit_events
            existing.validation_results = validation_results
            existing.notes             = notes
            existing.session_id        = session_id
        else:
            log = ReplicabilityLog(
                collection_hash    = collection_hash,
                session_id         = session_id,
                parameters         = parameters,
                statistics         = statistics,
                rate_limit_events  = rate_limit_events,
                validation_results = validation_results,
                notes              = notes,
            )
            db.add(log)

        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_replicability_logs(session_id: str = None, limit: int = 50) -> list:
    """Load replicability log records. session_id=None returns all sessions."""
    db = get_db_session()
    try:
        query = db.query(ReplicabilityLog)

        if session_id:
            query = query.filter_by(session_id=session_id)

        query = query.order_by(ReplicabilityLog.timestamp.desc()).limit(limit)

        results = query.all()

        return [
            {
                'collection_hash':   r.collection_hash,
                'timestamp':         r.timestamp.isoformat() if r.timestamp else None,
                'session_id':        r.session_id,
                'parameters':        r.parameters or {},
                'statistics':        r.statistics or {},
                'rate_limit_events': r.rate_limit_events or [],
                'validation_results': r.validation_results or {},
                'notes':             r.notes,
            }
            for r in results
        ]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Data quality report — used by thesis_export.py
# ---------------------------------------------------------------------------

def get_data_quality_report() -> dict:
    """
    Generate a summary quality report over all collected data.

    FIX: NSFW count previously used CollectedData.extra_metadata['nsfw'].astext
    which is a PostgreSQL-only JSON path operator and raises OperationalError
    on SQLite. Replaced with Python-level iteration matching the pattern used
    for removed_items and non_english_items, which already worked correctly.

    For 150–200 posts this is acceptably fast. For larger datasets consider
    adding indexed columns for these flags.
    """
    db = get_db_session()
    try:
        all_items = db.query(CollectedData).all()
        total_items = len(all_items)

        nsfw_items        = 0
        removed_items     = 0
        non_english_items = 0

        for item in all_items:
            meta = item.extra_metadata or {}
            if meta.get('nsfw') is True:
                nsfw_items += 1
            if meta.get('content_status') in ('removed', 'author_deleted'):
                removed_items += 1
            if meta.get('language_flag') == 'likely_non_english':
                non_english_items += 1

        return {
            'total_items':       total_items,
            'nsfw_items':        nsfw_items,
            'removed_items':     removed_items,
            'non_english_items': non_english_items,
            'report_generated':  datetime.utcnow().isoformat(),
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Zotero — inactive (future use)
# ---------------------------------------------------------------------------

def save_zotero_references(items: list, session_id: str) -> int:
    """Upsert Zotero reference records. Not used in core thesis pipeline."""
    db = get_db_session()
    try:
        saved_count = 0
        for item in items:
            existing = db.query(ZoteroReference).filter_by(
                zotero_key=item.get('zotero_key')
            ).first()

            if existing:
                existing.item_type    = item.get('item_type')
                existing.title        = item.get('title')
                existing.authors      = item.get('authors', [])
                existing.year         = item.get('year')
                existing.abstract     = item.get('abstract')
                existing.doi          = item.get('doi')
                existing.url          = item.get('url')
                existing.tags         = item.get('tags', [])
                existing.collections  = item.get('collections', [])
                existing.keywords     = item.get('keywords', [])
                existing.citation_apa = item.get('citation_apa')
                existing.synced_at    = datetime.utcnow()
                existing.session_id   = session_id
            else:
                record = ZoteroReference(
                    zotero_key   = item.get('zotero_key'),
                    item_type    = item.get('item_type'),
                    title        = item.get('title'),
                    authors      = item.get('authors', []),
                    year         = item.get('year'),
                    abstract     = item.get('abstract'),
                    doi          = item.get('doi'),
                    url          = item.get('url'),
                    tags         = item.get('tags', []),
                    collections  = item.get('collections', []),
                    keywords     = item.get('keywords', []),
                    citation_apa = item.get('citation_apa'),
                    session_id   = session_id,
                )
                db.add(record)

            saved_count += 1

        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_zotero_references(session_id: str = None, limit: int = 200) -> list:
    """Load Zotero reference records. Not used in core thesis pipeline."""
    db = get_db_session()
    try:
        query = db.query(ZoteroReference)

        if session_id:
            query = query.filter_by(session_id=session_id)

        query = query.order_by(ZoteroReference.synced_at.desc()).limit(limit)

        results = query.all()

        return [
            {
                'zotero_key':   r.zotero_key,
                'item_type':    r.item_type,
                'title':        r.title,
                'authors':      r.authors or [],
                'year':         r.year,
                'abstract':     r.abstract,
                'doi':          r.doi,
                'url':          r.url,
                'tags':         r.tags or [],
                'collections':  r.collections or [],
                'keywords':     r.keywords or [],
                'citation_apa': r.citation_apa,
                'synced_at':    r.synced_at.isoformat() if r.synced_at else None,
            }
            for r in results
        ]
    finally:
        db.close()


def get_all_zotero_keywords() -> list:
    """
    Return sorted list of unique keywords across all Zotero references.
    Called by modules/reddit_scraper.py to populate the Zotero keyword
    search suggestion UI.
    """
    db = get_db_session()
    try:
        results = db.query(ZoteroReference).all()
        all_keywords: set = set()
        for r in results:
            if r.keywords:
                all_keywords.update(r.keywords)
        return sorted(all_keywords)
    finally:
        db.close()


def save_citation_links(links: list, session_id: str) -> int:
    """Save Zotero citation-collection links. Not used in core thesis pipeline."""
    db = get_db_session()
    try:
        saved_count = 0
        for link in links:
            existing = db.query(ZoteroCollectionLink).filter_by(
                collection_hash=link.get('collection_hash'),
                zotero_key=link.get('zotero_key'),
            ).first()

            if not existing:
                record = ZoteroCollectionLink(
                    collection_hash  = link.get('collection_hash'),
                    zotero_key       = link.get('zotero_key'),
                    link_type        = link.get('link_type', 'manual'),
                    relevance_score  = link.get('relevance_score'),
                    matched_keywords = link.get('matched_keywords'),
                    session_id       = session_id,
                    notes            = link.get('notes'),
                )
                db.add(record)
                saved_count += 1

        db.commit()
        return saved_count
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()


def load_citation_links(collection_hash: str = None, limit: int = 100) -> list:
    """
    Load Zotero citation links with joined reference metadata.
    Not used in core thesis pipeline.

    NOTE: Performs N+1 queries (one ZoteroReference lookup per link row).
    Acceptable for small datasets; replace with a JOIN if performance matters.
    """
    db = get_db_session()
    try:
        query = db.query(ZoteroCollectionLink)

        if collection_hash:
            query = query.filter_by(collection_hash=collection_hash)

        query = query.order_by(ZoteroCollectionLink.linked_at.desc()).limit(limit)

        results = query.all()
        links_with_refs = []

        for r in results:
            ref = db.query(ZoteroReference).filter_by(
                zotero_key=r.zotero_key
            ).first()

            links_with_refs.append({
                'collection_hash':  r.collection_hash,
                'zotero_key':       r.zotero_key,
                'link_type':        r.link_type,
                'relevance_score':  r.relevance_score,
                'matched_keywords': r.matched_keywords or [],
                'linked_at':        r.linked_at.isoformat() if r.linked_at else None,
                'notes':            r.notes,
                'citation':         ref.citation_apa if ref else None,
                'title':            ref.title if ref else None,
                'year':             ref.year if ref else None,
            })

        return links_with_refs
    finally:
        db.close()
