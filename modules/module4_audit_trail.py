# modules/module4_audit_trail.py
"""
Audit Trail Module — NootropicRedditScrapePPM
==============================================
BQAH pipeline update to the existing audit trail.

Three responsibilities:
    1. migrate_existing_records()  — patches SQLite audit log for BQAH provenance
    2. AuditWriter                 — writes correct audit records for new coding sessions
    3. verify_privacy()            — enforces privacy guard before every write

Changes from original audit trail (PRAW → BQAH):
    - data_source:    'PRAW' | 'JSON_endpoint' → 'BQAH_for_researchers_external'
    - bigquery_job_id: NEW field (<REDDIT_PROJECT_ID>:US.script_job_[hash])
    - body_length:    NEW field (char count of post body — not the body itself)
    - non_english_flag: NEW field (bool from Module 1 language detection)
    - detected_language: NEW field (ISO language code if non_english_flag = True)

Audit log fields (full spec per Metaprompt 3):
    session_hash, coded_at, post_id, subreddit_id, corpus_layer, tertile,
    body_length, codes_assigned[], evidence_quotes[], confidence_ratings[],
    model, model_version, bigquery_job_id, data_source,
    researcher_override (bool), override_notes,
    non_english_flag (bool), detected_language

Privacy contract (non-negotiable):
    - Post body text NEVER written to audit log
    - author_id NEVER written to audit log
    - Evidence quotes written as researcher-selected extracts only
    - Username patterns in evidence quotes flagged (not auto-redacted)

Integration:
    This module wraps the existing log_action() from utils/db_helpers.py.
    It does not replace it — it adds BQAH-specific fields and enforces
    the privacy contract before delegating to the existing writer.

Usage:
    from modules.module4_audit_trail import AuditWriter, migrate_existing_records

    # One-time migration (run once per DB after BQAH extraction)
    migrate_existing_records(db_path="nootropic_ppm.db")

    # Per-session writer
    writer = AuditWriter(
        session_hash="abc123",
        bigquery_job_id="<REDDIT_PROJECT_ID>:US.script_job_abc123",
        model="llama3.1",
        model_version="3.1",
        db_path="nootropic_ppm.db",
    )
    writer.log(coded_post_dict)
"""

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_SOURCE_BQAH = "BQAH_for_researchers_external"
DATA_SOURCE_OLD  = {"PRAW", "JSON_endpoint", "praw", "json_endpoint"}

# Privacy — fields that must NEVER appear in any audit record
_FORBIDDEN_FIELDS = {"body", "title", "author_id", "author", "selftext"}

# Regex tripwire for username patterns in evidence quotes
# Catches u/username Reddit format
_USERNAME_PATTERN = re.compile(r"\bu/[A-Za-z0-9_]{3,20}\b")


# ---------------------------------------------------------------------------
# Privacy verification — called before every write
# ---------------------------------------------------------------------------

def verify_privacy(record: dict) -> tuple[bool, list[str]]:
    """
    Verify a prospective audit record against the privacy contract.

    Returns:
        (passed: bool, violations: list[str])
        passed=True means safe to write.
        violations is empty when passed=True.

    Checks:
        1. No forbidden field keys present (body, title, author_id etc.)
        2. No evidence_quote contains a Reddit username pattern (u/username)
        3. author_id absent from all nested structures
    """
    violations: list[str] = []

    # Check 1 — forbidden keys
    for key in record:
        if key.lower() in _FORBIDDEN_FIELDS:
            violations.append(
                f"FORBIDDEN FIELD: '{key}' must never appear in audit log."
            )

    # Check 2 — username pattern in evidence quotes
    quotes = record.get("evidence_quotes", [])
    if isinstance(quotes, str):
        try:
            quotes = json.loads(quotes)
        except (json.JSONDecodeError, TypeError):
            quotes = []

    for i, q in enumerate(quotes):
        if isinstance(q, str) and _USERNAME_PATTERN.search(q):
            violations.append(
                f"POTENTIAL USERNAME in evidence_quotes[{i}]: "
                f"contains u/ pattern — review before writing."
            )

    # Check 3 — author_id anywhere in record values
    record_str = json.dumps(record, default=str)
    if '"author_id"' in record_str or "'author_id'" in record_str:
        violations.append(
            "FORBIDDEN KEY: 'author_id' found in nested record structure."
        )

    passed = len(violations) == 0
    if not passed:
        logger.error(
            "Privacy verification FAILED for post_id=%s: %s",
            record.get("post_id", "UNKNOWN"),
            "; ".join(violations),
        )
    return passed, violations


# ---------------------------------------------------------------------------
# Schema migration — run once after BQAH extraction
# ---------------------------------------------------------------------------

def migrate_existing_records(db_path: str = "nootropic_ppm.db") -> dict:
    """
    One-time migration of existing SQLite audit log for BQAH provenance.

    Changes applied:
        1. ADD COLUMN bigquery_job_id TEXT (if not present)
        2. ADD COLUMN body_length INTEGER (if not present)
        3. ADD COLUMN non_english_flag BOOLEAN DEFAULT 0 (if not present)
        4. ADD COLUMN detected_language TEXT (if not present)
        5. UPDATE data_source = 'BQAH_for_researchers_external'
           WHERE data_source IN ('PRAW', 'JSON_endpoint') OR data_source IS NULL

    Safe to run multiple times — uses IF NOT EXISTS and conditional UPDATE.

    Returns:
        dict with migration summary (rows_updated, columns_added, errors)
    """
    summary = {
        "columns_added": [],
        "rows_updated":  0,
        "errors":        [],
        "db_path":       db_path,
        "migrated_at":   datetime.now(timezone.utc).isoformat(),
    }

    NEW_COLUMNS = [
        ("bigquery_job_id",  "TEXT",    None),
        ("body_length",      "INTEGER", None),
        ("non_english_flag", "INTEGER", 0),      # SQLite stores bool as int
        ("detected_language","TEXT",    None),
        ("data_source",      "TEXT",    None),
    ]

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.create_function("chr", 1, chr)
        conn.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))
        cur  = conn.cursor()

        # Discover existing audit table name
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('audit_log', 'coding_audit', 'audit', 'action_log')"
        )
        tables = [r["name"] for r in cur.fetchall()]
        if not tables:
            # Fallback: list all tables so researcher can identify correct one
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            all_tables = [r["name"] for r in cur.fetchall()]
            msg = (
                f"No known audit table found in '{db_path}'. "
                f"Tables present: {all_tables}. "
                "Pass the correct table name via audit_table_name parameter."
            )
            logger.error(msg)
            summary["errors"].append(msg)
            return summary

        audit_table = tables[0]
        logger.info("migrate_existing_records: target table = '%s'", audit_table)

        # Step 1 — add new columns (ignore if already present)
        cur.execute(f"PRAGMA table_info({audit_table})")
        existing_cols = {r["name"] for r in cur.fetchall()}

        for col_name, col_type, default in NEW_COLUMNS:
            if col_name not in existing_cols:
                default_clause = f" DEFAULT {default}" if default is not None else ""
                cur.execute(
                    f"ALTER TABLE {audit_table} "
                    f"ADD COLUMN {col_name} {col_type}{default_clause}"
                )
                summary["columns_added"].append(col_name)
                logger.info("Added column '%s %s' to %s.", col_name, col_type, audit_table)
            else:
                logger.debug("Column '%s' already present — skipping.", col_name)

        # Step 2 — update data_source to BQAH value
        placeholders = ",".join(f"'{s}'" for s in DATA_SOURCE_OLD)
        cur.execute(
            f"""
            UPDATE {audit_table}
               SET data_source = ?
             WHERE data_source IN ({placeholders})
                OR data_source IS NULL
            """,
            (DATA_SOURCE_BQAH,),
        )
        summary["rows_updated"] = cur.rowcount
        logger.info(
            "Updated data_source to '%s' for %d existing records.",
            DATA_SOURCE_BQAH, cur.rowcount,
        )

        conn.commit()
        logger.info("Migration complete: %s", summary)

    except sqlite3.Error as e:
        msg = f"SQLite error during migration: {e}"
        logger.error(msg)
        summary["errors"].append(msg)
    finally:
        if "conn" in dir():
            conn.close()

    return summary


# ---------------------------------------------------------------------------
# AuditWriter — per-session writer for new BQAH coding records
# ---------------------------------------------------------------------------

class AuditWriter:
    """
    Writes audit records for a single coding session.

    One AuditWriter per coding session (per run of Module 2).
    Instantiated with session-level provenance (session_hash, bigquery_job_id,
    model, model_version) and writes one record per coded post via .log().

    Wraps existing log_action() from utils/db_helpers if available.
    Falls back to direct SQLite write if db_helpers not importable.

    Privacy contract enforced via verify_privacy() before every write.
    Raises PrivacyViolationError if a record fails the privacy check.
    """

    class PrivacyViolationError(Exception):
        """Raised when a prospective audit record fails privacy verification."""
        pass

    def __init__(
        self,
        session_hash:      str,
        bigquery_job_id:   str,
        model:             str,
        model_version:     str,
        db_path:           str = "nootropic_ppm.db",
        audit_table_name:  str = "audit_log",
        dry_run:           bool = False,
        conn:              object = None,
    ):
        """
        Args:
            session_hash:     unique hash for this coding session
            bigquery_job_id:  BigQuery job ID for the extraction that produced
                              the corpus (format: <REDDIT_PROJECT_ID>:
                              US.script_job_[hash])
            model:            Ollama model name ('llama3.1' or 'gemma3:12b')
            model_version:    model version string
            db_path:          path to SQLite database
            audit_table_name: name of the audit log table
            dry_run:          if True, validate but do not write to DB
            conn:             optional existing sqlite3.Connection to share.
                              Pass bqah_coder's self.engine to avoid SQLite
                              "database is locked" errors from concurrent
                              connection attempts.
        """
        self.session_hash     = session_hash
        self.bigquery_job_id  = bigquery_job_id
        self.model            = model
        self.model_version    = model_version
        self.db_path          = db_path
        self.audit_table_name = audit_table_name
        self.dry_run          = dry_run
        self._shared_conn     = conn   # shared connection — avoids DB lock
        self._records_written = 0
        self._records_failed  = 0

        # Attempt to import existing db_helpers log_action
        self._log_action_fn = None
        try:
            from utils.db_helpers import log_action  # type: ignore
            self._log_action_fn = log_action
            logger.info("AuditWriter: using existing log_action from utils.db_helpers.")
        except ImportError:
            logger.info(
                "AuditWriter: utils.db_helpers not importable — "
                "using direct SQLite write."
            )

        logger.info(
            "AuditWriter initialised | session=%s | job=%s | model=%s %s | dry_run=%s",
            self.session_hash, self.bigquery_job_id,
            self.model, self.model_version, self.dry_run,
        )

    # ------------------------------------------------------------------

    def log(self, coded_post: dict, researcher_override: bool = False,
            override_notes: str = "") -> bool:
        """
        Write one audit record for a coded post.

        Args:
            coded_post:          dict — full output record from Module 2 coding
                                 (see coded output schema in Metaprompt 3)
            researcher_override: True if researcher manually overrode LLM codes
            override_notes:      explanation if researcher_override is True

        Returns:
            True if record written (or dry_run passed), False on failure.

        Raises:
            PrivacyViolationError if post body text or author_id detected.
        """
        record = self._build_record(coded_post, researcher_override, override_notes)

        # Privacy gate — hard stop, never bypass
        passed, violations = verify_privacy(record)
        if not passed:
            self._records_failed += 1
            raise self.PrivacyViolationError(
                f"Privacy violation for post {record.get('post_id')}: "
                + "; ".join(violations)
            )

        if self.dry_run:
            logger.debug("DRY RUN — record validated, not written: %s", record["post_id"])
            return True

        success = self._write(record)
        if success:
            self._records_written += 1
        else:
            self._records_failed += 1
        return success

    # ------------------------------------------------------------------

    def _build_record(
        self,
        coded_post: dict,
        researcher_override: bool,
        override_notes: str,
    ) -> dict:
        """
        Assemble the audit record from coded_post output.
        Extracts only audit-safe fields — body text is never included.
        """
        # Extract codes, quotes, and confidence from deductive_codes
        raw_codes = coded_post.get("deductive_codes", [])
        if isinstance(raw_codes, str):
            try:
                raw_codes = json.loads(raw_codes)
            except (json.JSONDecodeError, TypeError):
                raw_codes = []

        codes_assigned   = [c.get("code")           for c in raw_codes if isinstance(c, dict) and c.get("code")]
        evidence_quotes  = [c.get("evidence_quote")  for c in raw_codes if isinstance(c, dict) and c.get("evidence_quote")]
        confidence_ratings = [c.get("confidence")   for c in raw_codes if isinstance(c, dict) and c.get("confidence")]

        record = {
            # Post identification (no body, no author_id)
            "post_id":           coded_post.get("post_id"),
            "subreddit_id":      coded_post.get("subreddit_id"),
            "corpus_layer":      coded_post.get("corpus_layer"),
            "tertile":           coded_post.get("tertile"),
            "body_length":       coded_post.get("body_length"),   # char count only

            # Coding output
            "codes_assigned":      json.dumps(codes_assigned),
            "evidence_quotes":     json.dumps(evidence_quotes),
            "confidence_ratings":  json.dumps(confidence_ratings),

            # Session provenance
            "session_hash":      self.session_hash,
            "coded_at":          datetime.now(timezone.utc).isoformat(),
            "model":             self.model,
            "model_version":     self.model_version,
            "bigquery_job_id":   self.bigquery_job_id,
            "data_source":       DATA_SOURCE_BQAH,

            # Researcher override
            "researcher_override": int(researcher_override),
            "override_notes":      override_notes if researcher_override else "",

            # Language flags from Module 1
            "non_english_flag":   int(coded_post.get("non_english_flag", False)),
            "detected_language":  coded_post.get("detected_language", ""),
        }

        # Explicit safety check — confirm body not accidentally included
        assert "body" not in record, "CRITICAL: post body leaked into audit record."
        assert "author_id" not in record, "CRITICAL: author_id leaked into audit record."

        return record

    # ------------------------------------------------------------------

    def _write(self, record: dict) -> bool:
        """
        Write record to audit log via existing log_action or direct SQLite.
        """
        # Path A: use existing log_action if available
        if self._log_action_fn is not None:
            try:
                # Correct db_helpers.log_action() signature:
                # log_action(action, session_id, details)
                # session_hash is used as session_id for audit continuity
                self._log_action_fn(
                    action="coded_post_bqah",
                    session_id=self.session_hash,
                    details=record,
                )
                return True
            except Exception as e:
                logger.warning(
                    "log_action failed for %s: %s — falling back to direct write.",
                    record.get("post_id"), e,
                )

        # Path B: direct SQLite write
        return self._write_direct(record)

    def _write_direct(self, record: dict) -> bool:
        """
        Direct SQLite INSERT as fallback when log_action unavailable.

        Uses shared connection (self._shared_conn) if provided — avoids the
        'database is locked' error caused by bqah_coder.py holding an open
        write connection while AuditWriter attempts to open a second one.

        If no shared connection, opens a fresh connection with 30s timeout
        and WAL journal mode for better concurrency.
        """
        use_shared = self._shared_conn is not None
        try:
            if use_shared:
                conn = self._shared_conn
            else:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")

            conn.create_function("chr", 1, chr)
            conn.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))

            cur = conn.cursor()

            # Map known fields to audit_log core columns to mimic log_action
            record_for_db = record.copy()
            record_for_db['session_id'] = record_for_db.pop('session_hash', self.session_hash)
            record_for_db['timestamp'] = record_for_db.pop('coded_at', datetime.now(timezone.utc).isoformat())
            record_for_db['action'] = "coded_post_bqah"

            # Discover columns present in table
            cur.execute(f"PRAGMA table_info({self.audit_table_name})")
            existing_cols = {row[1] for row in cur.fetchall()}

            # Separate columns into existing and missing
            filtered = {}
            details_dict = {}
            for k, v in record_for_db.items():
                if k in existing_cols:
                    filtered[k] = v
                else:
                    details_dict[k] = v

            if 'details' in existing_cols:
                filtered['details'] = json.dumps(details_dict)
            elif details_dict:
                logger.warning(
                    "Table '%s' is missing 'details' column and other fields: %s",
                    self.audit_table_name, list(details_dict.keys())
                )

            if not filtered:
                logger.error(
                    "No matching columns found in '%s'. "
                    "Check table name and run migrate_existing_records().",
                    self.audit_table_name,
                )
                return False

            cols   = ", ".join(filtered.keys())
            ph     = ", ".join("?" * len(filtered))
            values = list(filtered.values())

            cur.execute(
                f"INSERT INTO {self.audit_table_name} ({cols}) VALUES ({ph})",
                values,
            )
            # Only commit if we own the connection — shared conn commits in caller
            if not use_shared:
                conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(
                "Direct SQLite write failed for post %s: %s",
                record.get("post_id"), e,
            )
            return False
        finally:
            if not use_shared and 'conn' in dir():
                conn.close()

    # ------------------------------------------------------------------

    def session_summary(self) -> dict:
        """
        Return a summary of this coding session for the audit log header.
        Call at end of session before closing.
        """
        return {
            "session_hash":     self.session_hash,
            "bigquery_job_id":  self.bigquery_job_id,
            "model":            self.model,
            "model_version":    self.model_version,
            "data_source":      DATA_SOURCE_BQAH,
            "records_written":  self._records_written,
            "records_failed":   self._records_failed,
            "session_closed_at": datetime.now(timezone.utc).isoformat(),
        }

    def __repr__(self) -> str:
        return (
            f"AuditWriter(session={self.session_hash[:8]}..., "
            f"written={self._records_written}, failed={self._records_failed})"
        )


# ---------------------------------------------------------------------------
# Convenience: validate existing audit log for privacy compliance
# ---------------------------------------------------------------------------

def audit_privacy_scan(db_path: str = "nootropic_ppm.db",
                       audit_table: str = "audit_log") -> dict:
    """
    Scan existing audit log records for potential privacy violations.

    Checks:
        - No rows contain body text (length > 500 chars in any text field)
        - No rows contain author_id values
        - No evidence_quotes contain Reddit username patterns

    Returns:
        dict with scan results — flagged_rows list and summary counts.
    """
    results = {
        "total_scanned":     0,
        "flagged_rows":      [],
        "body_text_found":   0,
        "author_id_found":   0,
        "username_pattern":  0,
        "scan_completed_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.create_function("chr", 1, chr)
        conn.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))
        cur  = conn.cursor()
        cur.execute(f"SELECT * FROM {audit_table}")
        rows = cur.fetchall()
        results["total_scanned"] = len(rows)

        for row in rows:
            row_dict  = dict(row)
            row_flags = []

            for key, val in row_dict.items():
                if not isinstance(val, str):
                    continue

                # Flag suspiciously long text fields (likely post body)
                if key not in ("override_notes", "evidence_quotes") and len(val) > 500:
                    row_flags.append(f"LONG_TEXT_FIELD: {key} ({len(val)} chars)")
                    results["body_text_found"] += 1

                # Flag author_id values
                if key == "author_id" or "author_id" in val:
                    row_flags.append(f"AUTHOR_ID_FIELD: {key}")
                    results["author_id_found"] += 1

                # Flag username patterns in evidence quotes
                if key == "evidence_quotes" and _USERNAME_PATTERN.search(val):
                    row_flags.append(f"USERNAME_PATTERN in {key}")
                    results["username_pattern"] += 1

            if row_flags:
                results["flagged_rows"].append({
                    "post_id": row_dict.get("post_id", "UNKNOWN"),
                    "flags":   row_flags,
                })

        logger.info(
            "Privacy scan complete: %d rows scanned, %d flagged.",
            results["total_scanned"],
            len(results["flagged_rows"]),
        )

    except sqlite3.Error as e:
        logger.error("Privacy scan SQLite error: %s", e)
        results["error"] = str(e)
    finally:
        if "conn" in dir():
            conn.close()

    return results
