import argparse
import csv
import json
import logging
import sys
from datetime import datetime, date, timezone
from pathlib import Path

# ── SQLAlchemy imports (matches core/database.py) ─────────────────────────────
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker

# ── Confirmed subreddit ID → name map (locked from BQAH session) ──────────────
SUBREDDIT_MAP = {
    't5_2r81c':  'Nootropics',
    't5_2qhb8':  'Supplements',
    't5_2v89v':  'Decaf',
    't5_2vnoe':  'Biohackers',
    't5_4aoxhu': 'NooTopics',
    't5_2ttk1':  'StackAdvice',
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bqah_import.log', encoding='utf-8'),
    ]
)
log = logging.getLogger('bqah_import')


# ══════════════════════════════════════════════════════════════════════════════
# CLEANING HELPERS — one function per misalignment
# ══════════════════════════════════════════════════════════════════════════════

def clean_score(raw: str) -> int:
    """Fix #4 — score is a STRING in both files."""
    try:
        return int(float(raw))          # float() first handles '1525.0' edge case
    except (ValueError, TypeError):
        return 0


def clean_ratio(raw: str) -> float | None:
    """Fix #5 — upvote_ratio is a high-precision STRING."""
    if not raw or raw.strip() == '':
        return None
    try:
        return round(float(raw), 6)
    except (ValueError, TypeError):
        return None


def clean_bool(raw: str) -> bool:
    """Fix #6 — booleans arrive as lowercase strings 'true'/'false'."""
    return str(raw).strip().lower() == 'true'


def clean_text_field(raw: str) -> str | None:
    """Fix #7 — empty strings, 'NaN', 'None' → Python None."""
    if raw is None:
        return None
    stripped = raw.strip()
    if stripped in ('', 'NaN', 'nan', 'None', 'none', 'null', 'NULL'):
        return None
    return stripped


def clean_permalink(raw: str) -> str:
    """Fix #8 — normalise permalink: always strip leading slash."""
    if not raw:
        return raw
    return raw.lstrip('/')


def clean_parent_id(raw: str) -> str | None:
    """Fix #9 — empty string for top-level comments → None."""
    cleaned = clean_text_field(raw)
    return cleaned if cleaned else None


def date_to_utc_float(date_str: str) -> float:
    """
    Convert 'YYYY-MM-DD' string to Unix UTC float.
    BigQuery DATE columns have no time component — we use midnight UTC.
    """
    try:
        d = date.fromisoformat(date_str)
        return float(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())
    except (ValueError, TypeError):
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# CSV READERS
# ══════════════════════════════════════════════════════════════════════════════

def read_posts(path: Path) -> dict[str, dict]:
    """
    Read posts CSV. Returns dict keyed by reddit_id for O(1) lookup
    when enriching comments.
    """
    posts = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            reddit_id = row['id']
            posts[reddit_id] = {
                # Identity
                'reddit_id':     reddit_id,
                'type':          'submission',

                # Fix #1 (posts already have subreddit_name — no lookup needed)
                'subreddit':     row.get('subreddit_name') or
                                 SUBREDDIT_MAP.get(row.get('subreddit_id', ''), ''),
                'subreddit_id':  row.get('subreddit_id', ''),

                # Content
                'title':         clean_text_field(row.get('title', '')),
                'text':          clean_text_field(row.get('body', '')),
                'author':        None,           # author_id excluded per Addendum

                # Numerics — Fix #4, #5
                'score':         clean_score(row.get('score', '0')),
                'upvote_ratio':  clean_ratio(row.get('upvote_ratio', '')),

                # Timestamps
                'created_utc':   date_to_utc_float(row.get('post_date', '')),

                # Routing
                'permalink':     clean_permalink(row.get('permalink', '')),
                'post_id':       None,           # posts have no parent post

                # Corpus metadata — stored in extra_metadata
                'corpus_layer':  row.get('corpus_layer', ''),
                'tertile':       int(row['tertile']) if row.get('tertile', '').isdigit() else None,

                # Fix #6, #7
                'nsfw':          clean_bool(row.get('nsfw', 'false')),
                'locked':        clean_bool(row.get('locked', 'false')),
                'flair_text':    clean_text_field(row.get('flair_text', '')),
            }
    log.info(f"Read {len(posts):,} posts from {path.name}")
    return posts


def read_comments(paths: list[Path], posts_index: dict[str, dict]) -> list[dict]:
    """
    Read one or more comment CSVs. Merges transparently.
    Fix #2: both Nootropics and remaining5 files processed identically.
    Fix #3: tertile enriched from posts_index JOIN on post_id.
    Fix #10: posts with no comments logged, not errored.
    """
    comments = []
    seen_ids = set()
    orphan_post_ids = set()

    for path in paths:
        file_count = 0
        dup_count  = 0
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                comment_id = row.get('comment_id', row.get('id', ''))

                # Deduplicate (handles OFFSET boundary overlap from extraction)
                if comment_id in seen_ids:
                    dup_count += 1
                    continue
                seen_ids.add(comment_id)

                post_id = row.get('post_id', '')

                # Fix #1 — resolve subreddit_name from ID map
                sub_id   = row.get('subreddit_id', '')
                sub_name = SUBREDDIT_MAP.get(sub_id, sub_id)

                # Fix #3 — enrich tertile from posts index JOIN
                parent_post = posts_index.get(post_id)
                if parent_post:
                    tertile       = parent_post.get('tertile')
                    upvote_ratio  = parent_post.get('upvote_ratio')  # comments have no ratio
                else:
                    tertile      = None
                    upvote_ratio = None
                    orphan_post_ids.add(post_id)

                comments.append({
                    # Identity
                    'reddit_id':    comment_id,
                    'type':         'comment',

                    # Fix #1
                    'subreddit':    sub_name,
                    'subreddit_id': sub_id,

                    # Content
                    'title':        None,        # comments have no title
                    'text':         clean_text_field(row.get('body', '')),
                    'author':       None,         # excluded per Addendum

                    # Fix #4
                    'score':        clean_score(row.get('score', '0')),

                    # Timestamps
                    'created_utc':  date_to_utc_float(row.get('comment_date', '')),

                    # Threading
                    'permalink':    clean_permalink(row.get('permalink', '')),
                    'post_id':      post_id or None,
                    'parent_id':    clean_parent_id(row.get('parent_id', '')),  # Fix #9

                    # Fix #6
                    'gilded':       clean_bool(row.get('gilded', 'false')),

                    # Corpus metadata
                    'corpus_layer': row.get('corpus_layer', ''),
                    'tertile':      tertile,      # Fix #3
                })
                file_count += 1

        log.info(f"Read {file_count:,} comments from {path.name}  "
                 f"(duplicates skipped: {dup_count})")

    # Fix #10 — log orphan post_ids (comments whose parent post is not in corpus)
    if orphan_post_ids:
        log.warning(
            f"Fix #10 — {len(orphan_post_ids)} post_id(s) in comments have no "
            f"matching post in corpus (expected for 3 zero-comment posts). "
            f"Orphan post_ids: {sorted(orphan_post_ids)[:10]}"
        )

    log.info(f"Total unique comments loaded: {len(comments):,}")
    return comments


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE WRITER
# ══════════════════════════════════════════════════════════════════════════════

INSERT_SQL = text("""
    INSERT OR IGNORE INTO collected_data (
        reddit_id, type, subreddit, title, text,
        author, score, created_utc, num_comments,
        url, permalink, post_id, collected_at,
        session_id, extra_metadata
    ) VALUES (
        :reddit_id, :type, :subreddit, :title, :text,
        :author, :score, :created_utc, :num_comments,
        :url, :permalink, :post_id, :collected_at,
        :session_id, :extra_metadata
    )
""")

AUDIT_SQL = text("""
    INSERT INTO audit_log (timestamp, action, session_id, user_info, details)
    VALUES (:timestamp, :action, :session_id, :user_info, :details)
""")


def build_extra_metadata(record: dict) -> str:
    """
    Pack corpus-specific fields that have no direct column in collected_data
    into the JSON extra_metadata field.
    """
    meta = {
        'corpus_layer':  record.get('corpus_layer'),
        'tertile':       record.get('tertile'),
        'source':        'BQAH_BigQuery_Analytics_Hub',
        'addendum':      '<ADDENDUM_ID>',
        'subreddit_id':  record.get('subreddit_id'),
    }
    # Post-only fields
    if record['type'] == 'submission':
        meta.update({
            'upvote_ratio': record.get('upvote_ratio'),
            'nsfw':         record.get('nsfw'),
            'locked':       record.get('locked'),
            'flair_text':   record.get('flair_text'),
        })
    # Comment-only fields
    if record['type'] == 'comment':
        meta.update({
            'gilded':     record.get('gilded'),
            'parent_id':  record.get('parent_id'),
        })
    return json.dumps(meta, ensure_ascii=False)


def write_to_db(records: list[dict], db_path: Path, session_id: str) -> dict:
    """Write records to SQLite collected_data table. Returns stats dict."""
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={
            'check_same_thread': False,
            'timeout': 30
        }
    )
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        # Register custom chr/char functions to guarantee compatibility in older/custom SQLite environments
        dbapi_connection.create_function("chr", 1, chr)
        dbapi_connection.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))
        
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()
    Session = sessionmaker(bind=engine)
    db = Session()

    now = datetime.utcnow()
    inserted = skipped = errors = 0

    try:
        batch = []
        for rec in records:
            try:
                params = {
                    'reddit_id':     rec['reddit_id'],
                    'type':          rec['type'],
                    'subreddit':     rec.get('subreddit', ''),
                    'title':         rec.get('title'),
                    'text':          rec.get('text'),
                    'author':        None,           # always None — Addendum compliance
                    'score':         rec.get('score', 0),
                    'created_utc':   rec.get('created_utc', 0.0),
                    'num_comments':  None,
                    'url':           None,
                    'permalink':     rec.get('permalink'),
                    'post_id':       rec.get('post_id'),
                    'collected_at':  now,
                    'session_id':    session_id,
                    'extra_metadata': build_extra_metadata(rec),
                }
                batch.append(params)
            except Exception as e:
                log.error(f"Build error on {rec.get('reddit_id','?')}: {e}")
                errors += 1

            # Flush every 1,000 records to avoid huge transactions
            if len(batch) >= 1000:
                result = db.execute(INSERT_SQL, batch)
                inserted += result.rowcount
                skipped  += len(batch) - result.rowcount
                db.commit()
                batch = []

        # Final flush
        if batch:
            result = db.execute(INSERT_SQL, batch)
            inserted += result.rowcount
            skipped  += len(batch) - result.rowcount
            db.commit()

        # Audit log entry
        stats = {
            'inserted': inserted,
            'skipped_duplicates': skipped,
            'errors': errors,
            'total_processed': inserted + skipped + errors,
        }
        db.execute(AUDIT_SQL, {
            'timestamp':  now,
            'action':     'bqah_corpus_import',
            'session_id': session_id,
            'user_info':  'vladislav.dolgov@redditresearchers.com',
            'details':    json.dumps({
                **stats,
                'source_files': 'posts_export_20260504 + comments x2',
                'addendum':     '<ADDENDUM_ID>',
                'fixes_applied': [
                    'subreddit_name_lookup',
                    'comment_file_merge',
                    'tertile_join_from_posts',
                    'score_int_cast',
                    'upvote_ratio_float_cast',
                    'boolean_string_normalise',
                    'flair_text_null_normalise',
                    'permalink_slash_strip',
                    'parent_id_null_normalise',
                    'orphan_post_logged',
                ]
            })
        })
        db.commit()

    except Exception as e:
        db.rollback()
        log.error(f"Database write failed: {e}")
        raise
    finally:
        db.close()

    return stats


# ══════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Import BQAH corpus CSVs into NootropicRedditScrapePPM database'
    )
    parser.add_argument('--posts',    required=True,
                        help='Path to posts_export_YYYYMMDD.csv')
    parser.add_argument('--comments', required=True, nargs='+',
                        help='Path(s) to comment CSV(s) — pass both files if split')
    parser.add_argument('--db',       default='data/research_data.db',
                        help='Path to SQLite database (default: data/research_data.db)')
    parser.add_argument('--session',  default=f'BQAH_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}',
                        help='Session ID tag for audit trail')
    args = parser.parse_args()

    posts_path    = Path(args.posts)
    comment_paths = [Path(p) for p in args.comments]
    db_path       = Path(args.db)

    # ── Validate inputs ───────────────────────────────────────────────────────
    for p in [posts_path, *comment_paths]:
        if not p.exists():
            log.error(f"File not found: {p}")
            sys.exit(1)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("BQAH CORPUS IMPORT")
    log.info(f"Session:  {args.session}")
    log.info(f"Posts:    {posts_path}")
    log.info(f"Comments: {[str(p) for p in comment_paths]}")
    log.info(f"Database: {db_path}")
    log.info("=" * 60)

    # ── Load ──────────────────────────────────────────────────────────────────
    posts_index = read_posts(posts_path)
    comments    = read_comments(comment_paths, posts_index)

    # ── Merge into single record list ─────────────────────────────────────────
    all_records = list(posts_index.values()) + comments
    log.info(f"Total records to import: {len(all_records):,} "
             f"({len(posts_index):,} posts + {len(comments):,} comments)")

    # ── Write ─────────────────────────────────────────────────────────────────
    stats = write_to_db(all_records, db_path, args.session)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("IMPORT COMPLETE")
    log.info(f"  Inserted:            {stats['inserted']:,}")
    log.info(f"  Skipped (duplicate): {stats['skipped_duplicates']:,}")
    log.info(f"  Errors:              {stats['errors']:,}")
    log.info(f"  Total processed:     {stats['total_processed']:,}")
    log.info("=" * 60)

    if stats['errors'] > 0:
        log.warning(f"{stats['errors']} records failed — check bqah_import.log for details")
        sys.exit(1)


if __name__ == '__main__':
    main()
