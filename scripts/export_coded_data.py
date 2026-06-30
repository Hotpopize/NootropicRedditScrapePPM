#!/usr/bin/env python3
# scripts/export_coded_data.py
"""
Bridge Script — SQLite → CSV Export
=====================================
Reads coded output from data/research_data.db (written by bqah_coder.py)
and exports two clean CSV files for module3_aggregation.py consumption:

    outputs/coded_posts.csv           — all coded posts (Layer A + Layer B)
    outputs/emergent_candidates.csv   — Layer B emergent candidates only

Schema source: core/database.py + scripts/bqah_coder.py (confirmed read).

Key mapping decisions:
    coded_data.reddit_id            → post_id
    collected_data.subreddit        → subreddit_name  (via JOIN)
    coded_data.coding_approach      → corpus_layer    ('A_keyword'|'B_emergent')
    coded_data.extra_metadata JSON  → tertile, deductive_codes, circuit_breaker_flags
    len(collected_data.text)        → body_length     (char count only — body never exported)
    coded_data.themes JSON          → emergent_candidates (Layer B raw LLM output)

Privacy contract (non-negotiable):
    coded_data.raw_prompt     — NEVER exported (contains full post text in prompt)
    coded_data.raw_response   — NEVER exported (contains full LLM response with post text)
    collected_data.text       — NEVER exported (body content)
    collected_data.title      — NEVER exported
    collected_data.author     — NEVER exported
    Only post_id, metadata, and pre-extracted code/quote fields are written.

Usage:
    python scripts/export_coded_data.py
    python scripts/export_coded_data.py --db data/research_data.db --out outputs/
    python scripts/export_coded_data.py --dry-run    (schema inspection, no export)
    python scripts/export_coded_data.py --validate   (validate existing CSVs)
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = Path("data/research_data.db")
DEFAULT_OUT_DIR = Path("outputs")

# Columns that must NEVER appear in any output CSV
# raw_prompt and raw_response contain full post text embedded in LLM prompts
FORBIDDEN_EXPORT_COLS = {
    "raw_prompt", "raw_response",       # full post text in prompt/response
    "text", "title", "selftext",        # body content
    "author", "author_id", "username",  # identity fields
    "body",                             # alias for text
}


# ---------------------------------------------------------------------------
# SQL — confirmed against core/database.py schema
# ---------------------------------------------------------------------------

# Fetches all coded posts joined with collected_data for subreddit and body length.
# Extracts corpus_layer and tertile from extra_metadata JSON.
# raw_prompt and raw_response are deliberately excluded from SELECT.
CODED_POSTS_SQL = """
SELECT
    cd.reddit_id                                        AS post_id,
    col.subreddit                                       AS subreddit_name,
    cd.coding_approach                                  AS corpus_layer,
    cd.confidence                                       AS overall_confidence,
    cd.ppm_category                                     AS ppm_category,
    cd.ppm_subcodes                                     AS ppm_subcodes,
    cd.evidence_quotes                                  AS evidence_quotes_flat,
    cd.coded_at                                         AS coded_at,
    cd.coded_by                                         AS model,
    cd.session_id                                       AS session_id,
    cd.rationale                                        AS rationale,
    cd.extra_metadata                                   AS extra_metadata_raw,
    LENGTH(COALESCE(col.text, ''))                      AS body_length,
    col.created_utc                                     AS post_created_utc,
    col.score                                           AS post_score,
    col.permalink                                       AS permalink,
    col.data_source                                     AS data_source
FROM coded_data cd
LEFT JOIN collected_data col
       ON cd.reddit_id = col.reddit_id
ORDER BY cd.id ASC
"""

# Fetches Layer B rows for emergent candidate extraction from coded_data.themes.
# coding_approach = 'B_emergent' per bqah_coder.py persist_coded_data().
# Excludes test posts (data_source = 'json_endpoint').
EMERGENT_SOURCE_SQL = """
SELECT
    cd.reddit_id    AS post_id,
    col.subreddit   AS subreddit_name,
    cd.themes       AS themes_raw,
    cd.session_id   AS session_id
FROM coded_data cd
LEFT JOIN collected_data col
       ON cd.reddit_id = col.reddit_id
WHERE cd.coding_approach = 'B_emergent'
  AND cd.themes IS NOT NULL
  AND cd.themes != '[]'
  AND cd.themes != 'null'
  AND (col.data_source IS NULL OR col.data_source != 'json_endpoint')
ORDER BY cd.id ASC
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_json_field(val, fallback):
    """Safely parse a JSON field — returns fallback on any failure."""
    if val is None:
        return fallback
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return fallback


def _safe_dumps(val) -> str:
    """Convert any value to a JSON string for CSV storage."""
    if isinstance(val, str):
        # Already a JSON string — validate it
        try:
            json.loads(val)
            return val
        except (json.JSONDecodeError, TypeError):
            return "[]"
    try:
        return json.dumps(val)
    except (TypeError, ValueError):
        return "[]"


def _strip_forbidden(df: pd.DataFrame) -> pd.DataFrame:
    """Hard privacy gate — remove any forbidden columns before writing."""
    to_drop = [c for c in df.columns if c.lower() in FORBIDDEN_EXPORT_COLS]
    if to_drop:
        logger.error(
            "PRIVACY GATE: Dropping forbidden columns before export: %s", to_drop
        )
        df = df.drop(columns=to_drop)
    return df


# ---------------------------------------------------------------------------
# Export 1 — coded_posts.csv
# ---------------------------------------------------------------------------

def export_coded_posts(conn: sqlite3.Connection, out_path: Path) -> pd.DataFrame:
    """
    Build coded_posts.csv from coded_data JOIN collected_data.

    The critical field for module3_aggregation.py is `deductive_codes` —
    a JSON list of {code, evidence_quote, confidence} dicts. This lives
    inside coded_data.extra_metadata['deductive_codes'] as written by
    bqah_coder.py persist_coded_data().

    Returns the exported DataFrame for downstream validation.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(coded_data)")
    cols = {row[1] for row in cursor.fetchall()}
    
    subcodes_col = "COALESCE(cd.ppm_subcodes_clean, cd.ppm_subcodes)" if "ppm_subcodes_clean" in cols else "cd.ppm_subcodes"
    quotes_col = "COALESCE(cd.evidence_quotes_clean, cd.evidence_quotes)" if "evidence_quotes_clean" in cols else "cd.evidence_quotes"
    quarantine_col = ", cd.quarantine_reason" if "quarantine_reason" in cols else ", NULL AS quarantine_reason"
    where_clause = "WHERE cd.quarantine_reason IS NULL" if "quarantine_reason" in cols else "WHERE (col.data_source IS NULL OR col.data_source != 'json_endpoint')"

    
    query = f"""
SELECT
    cd.reddit_id                                        AS post_id,
    col.subreddit                                       AS subreddit_name,
    cd.coding_approach                                  AS corpus_layer,
    cd.confidence                                       AS overall_confidence,
    cd.ppm_category                                     AS ppm_category,
    {subcodes_col}                                      AS ppm_subcodes,
    {quotes_col}                                        AS evidence_quotes_flat,
    cd.coded_at                                         AS coded_at,
    cd.coded_by                                         AS model,
    cd.session_id                                       AS session_id,
    cd.rationale                                        AS rationale,
    cd.extra_metadata                                   AS extra_metadata_raw,
    LENGTH(COALESCE(col.text, ''))                      AS body_length,
    col.created_utc                                     AS post_created_utc,
    col.score                                           AS post_score,
    col.permalink                                       AS permalink,
    col.data_source                                     AS data_source
    {quarantine_col}
FROM coded_data cd
LEFT JOIN collected_data col
       ON cd.reddit_id = col.reddit_id
{where_clause}
ORDER BY cd.id ASC
"""
    logger.info("Querying coded_data + collected_data...")
    
    # Check for raw fallback leakage on certified rows
    if "ppm_subcodes_clean" in cols:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM coded_data 
            WHERE quarantine_reason IS NULL 
              AND coded_by = 'llama3.1' 
              AND ppm_subcodes_clean IS NULL 
              AND ppm_subcodes IS NOT NULL
        """)
        fallback_rows = cursor.fetchone()[0]
        if fallback_rows > 0:
            logger.error("CRITICAL: %d certified rows fell back to raw ppm_subcodes! Uncertified data leaking.", fallback_rows)
            raise ValueError(f"Clean database fallback leak detected on {fallback_rows} rows.")
        else:
            logger.info("Success: 0 certified rows fell back to raw ppm_subcodes.")
            
    df = pd.read_sql_query(query, conn)
    logger.info("Raw query returned %d rows.", len(df))

    if df.empty:
        raise RuntimeError(
            "coded_data table is empty. "
            "Run bqah_coder.py --run first."
        )

    # --- Extract fields from extra_metadata JSON ---
    # extra_metadata structure (per bqah_coder.py persist_coded_data):
    # {
    #   "corpus_layer":          "A_keyword" | "B_emergent",
    #   "circuit_breaker_flags": [...],
    #   "deductive_codes":       [{code, evidence_quote, confidence}, ...],
    #   "override_notes":        "..." (optional),
    #   "tertile":               1 | 2 | 3   (from collected_data.extra_metadata)
    # }

    def _extract_meta(row):
        meta = _parse_json_field(row["extra_metadata_raw"], {})
        if "ppm_subcodes_clean" in cols:
            clean_subcodes = _parse_json_field(row["ppm_subcodes"], [])
            clean_quotes = _parse_json_field(row["evidence_quotes_flat"], [])
            raw_deductive = meta.get("deductive_codes", [])
            raw_conf = {}
            if isinstance(raw_deductive, list):
                for item in raw_deductive:
                    if isinstance(item, dict) and "code" in item:
                        raw_conf[item["code"]] = item.get("confidence", "HIGH")
                    elif isinstance(item, str):
                        raw_conf[item] = "HIGH"
            elif isinstance(raw_deductive, dict):
                for code in raw_deductive.keys():
                    raw_conf[code] = "HIGH"
            deductive_codes = []
            for i, code in enumerate(clean_subcodes):
                quote = clean_quotes[i] if i < len(clean_quotes) else ""
                deductive_codes.append({
                    "code": code,
                    "evidence_quote": quote,
                    "confidence": raw_conf.get(code, "HIGH")
                })
        else:
            deductive_codes = meta.get("deductive_codes", [])

        return {
            "deductive_codes":       _safe_dumps(deductive_codes),
            "tertile":               meta.get("tertile", 2),
            "circuit_breaker_flags": _safe_dumps(meta.get("circuit_breaker_flags", [])),
            "override_notes":        meta.get("override_notes", ""),
            # corpus_layer from extra_metadata as primary, coding_approach as fallback
            "corpus_layer_meta":     meta.get("corpus_layer", row.get("corpus_layer", "A_keyword")),
            "machine_partition":     meta.get("machine_partition"),
        }

    logger.info("Extracting fields from extra_metadata JSON...")
    meta_expanded = df.apply(_extract_meta, axis=1, result_type="expand")
    df = pd.concat([df, meta_expanded], axis=1)

    # Use corpus_layer_meta as the authoritative corpus_layer
    df["corpus_layer"] = df["corpus_layer_meta"].fillna(df["corpus_layer"])
    df = df.drop(columns=["extra_metadata_raw", "corpus_layer_meta"])

    # --- Validate corpus_layer ---
    valid_layers = {"A_keyword", "B_emergent"}
    bad = df[~df["corpus_layer"].isin(valid_layers)]
    if not bad.empty:
        logger.warning(
            "%d rows have unexpected corpus_layer values: %s",
            len(bad), bad["corpus_layer"].unique().tolist(),
        )

    # --- Validate tertile ---
    df["tertile"] = pd.to_numeric(df["tertile"], errors="coerce").fillna(2).astype(int)
    bad_t = df[~df["tertile"].isin([1, 2, 3])]
    if not bad_t.empty:
        logger.warning(
            "%d rows have unexpected tertile values: %s",
            len(bad_t), bad_t["tertile"].unique().tolist(),
        )

    # --- Drop rows with null post_id ---
    null_ids = df["post_id"].isna().sum()
    if null_ids > 0:
        logger.warning("Dropping %d rows with null post_id.", null_ids)
        df = df.dropna(subset=["post_id"])

    # --- Privacy gate ---
    df = _strip_forbidden(df)

    # --- Final column order for module3_aggregation.py ---
    priority_cols = [
        "post_id", "subreddit_name", "corpus_layer", "tertile",
        "body_length", "deductive_codes", "overall_confidence",
        "ppm_category", "circuit_breaker_flags", "override_notes",
        "model", "session_id", "coded_at", "post_score",
        "post_created_utc", "permalink", "data_source", "machine_partition",
        "quarantine_reason",
    ]
    remaining = [c for c in df.columns if c not in priority_cols]
    ordered   = [c for c in priority_cols if c in df.columns] + remaining
    df = df[ordered]

    # --- Write ---
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(
        "[SUCCESS] coded_posts.csv written: %d rows, %d columns -> %s",
        len(df), len(df.columns), out_path,
    )

    # Corpus layer distribution summary
    layer_dist = df["corpus_layer"].value_counts().to_dict()
    tertile_dist = df["tertile"].value_counts().sort_index().to_dict()
    community_dist = df["subreddit_name"].value_counts().to_dict()
    logger.info("  corpus_layer: %s", layer_dist)
    logger.info("  tertile:      %s", tertile_dist)
    logger.info("  communities:  %s", community_dist)

    return df


# ---------------------------------------------------------------------------
# Export 2 — emergent_candidates.csv
# ---------------------------------------------------------------------------

def export_emergent_candidates(
    conn: sqlite3.Connection,
    out_path: Path,
) -> pd.DataFrame:
    """
    Build emergent_candidates.csv from coded_data.themes (Layer B rows only).

    coded_data.themes is a JSON list of emergent candidate dicts written by
    bqah_coder.py code_layer_b_pass2(). Each dict has the shape:
        {
            "label":                "...",
            "definition":           "...",
            "partial_overlap_with": "MOOR-F-03" | null,
            "distinct_dimension":   "...",
            "evidence_quote":       "...",
            "confidence":           "HIGH|MED|LOW"
        }

    NOTE: The `emergent_candidates` TABLE in the DB is for codebook-approved
    entries (status: pending/approved/rejected) — it is a different concept.
    Raw LLM output lives in coded_data.themes. This function reads themes.

    Returns the exported DataFrame for downstream validation.
    """
    logger.info("Querying Layer B themes for emergent candidates...")
    
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(coded_data)")
    cols = {row[1] for row in cursor.fetchall()}
    
    where_clause = "AND cd.quarantine_reason IS NULL" if "quarantine_reason" in cols else "AND (col.data_source IS NULL OR col.data_source != 'json_endpoint')"
    
    query = f"""
SELECT
    cd.reddit_id    AS post_id,
    col.subreddit   AS subreddit_name,
    cd.themes       AS themes_raw,
    cd.session_id   AS session_id
FROM coded_data cd
LEFT JOIN collected_data col
       ON cd.reddit_id = col.reddit_id
WHERE cd.coding_approach = 'B_emergent'
  AND cd.themes IS NOT NULL
  AND cd.themes != '[]'
  AND cd.themes != 'null'
  {where_clause}
ORDER BY cd.id ASC
"""
    source_df = pd.read_sql_query(query, conn)
    logger.info("Found %d Layer B posts with non-empty themes.", len(source_df))

    records = []
    for _, row in source_df.iterrows():
        themes = _parse_json_field(row["themes_raw"], [])
        if not isinstance(themes, list):
            continue

        for candidate in themes:
            if not isinstance(candidate, dict):
                continue
            label = candidate.get("label")
            if not label or not isinstance(label, str):
                continue
            label = label.strip()

            records.append({
                "label":                label,
                "definition":           candidate.get("definition") or "",
                "partial_overlap_with": candidate.get("partial_overlap_with") or "",
                "distinct_dimension":   candidate.get("distinct_dimension") or "",
                "evidence_quote":       candidate.get("evidence_quote") or "",
                "confidence":           candidate.get("confidence") or "",
                "subreddit_name":       row.get("subreddit_name", "UNKNOWN"),
                "corpus_layer":         "B_emergent",
                "source_post_id":       row.get("post_id", ""),
                "session_id":           row.get("session_id", ""),
            })

    if not records:
        logger.warning(
            "No emergent candidates found in Layer B themes. "
            "This is expected if Layer B coding has not yet run. "
            "An empty CSV will be written."
        )
        df = pd.DataFrame(columns=[
            "label", "definition", "partial_overlap_with", "distinct_dimension",
            "evidence_quote", "confidence", "subreddit_name", "corpus_layer",
            "source_post_id", "session_id",
        ])
    else:
        df = pd.DataFrame(records)
        logger.info("Extracted %d emergent candidate rows from Layer B themes.", len(df))

    # --- Privacy gate ---
    df = _strip_forbidden(df)

    # --- Write ---
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(
        "[SUCCESS] emergent_candidates.csv written: %d rows -> %s",
        len(df), out_path,
    )

    if not df.empty:
        label_counts = df["label"].value_counts().head(10).to_dict()
        logger.info("  Top candidate labels: %s", label_counts)

    return df


# ---------------------------------------------------------------------------
# Dry run — schema inspection only
# ---------------------------------------------------------------------------

def dry_run_inspect(conn: sqlite3.Connection, db_path: Path):
    """Print full schema inspection without writing any files."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]

    print(f"\n{'='*65}")
    print(f"DRY RUN — Schema inspection: {db_path}")
    print(f"{'='*65}")
    print(f"Tables found: {tables}\n")

    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        row_count = cur.fetchone()[0]
        print(f"  [{t}] — {row_count} rows")
        for c in cols:
            col_name = c[1]
            col_type = c[2]
            forbidden = " [!] FORBIDDEN - will be excluded from export" \
                if col_name.lower() in FORBIDDEN_EXPORT_COLS else ""
            print(f"    {col_name} ({col_type}){forbidden}")
        print()

    # Spot check: confirm extra_metadata has corpus_layer and tertile
    print("  Spot check - extra_metadata structure on first coded_data row:")
    try:
        cur.execute("SELECT extra_metadata FROM coded_data LIMIT 1")
        row = cur.fetchone()
        if row and row[0]:
            meta = _parse_json_field(row[0], {})
            print(f"    corpus_layer:          {meta.get('corpus_layer', 'MISSING')}")
            print(f"    tertile:               {meta.get('tertile', 'MISSING')}")
            print(f"    deductive_codes count: {len(meta.get('deductive_codes', []))}")
            print(f"    circuit_breaker_flags: {meta.get('circuit_breaker_flags', [])}")
        else:
            print("    coded_data table is empty - run bqah_coder.py first.")
    except sqlite3.OperationalError as e:
        print(f"    Could not inspect: {e}")

    print(f"\n{'='*65}\n")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_exports(coded_path: Path, emergent_path: Path) -> bool:
    """Post-export validation — confirms CSVs are readable and correctly shaped."""
    all_pass = True

    print(f"\n{'='*65}")
    print("EXPORT VALIDATION REPORT")
    print(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*65}")

    specs = [
        (
            "coded_posts.csv", coded_path,
            ["post_id", "subreddit_name", "corpus_layer",
             "tertile", "body_length", "deductive_codes"],
        ),
        (
            "emergent_candidates.csv", emergent_path,
            ["label", "definition", "subreddit_name", "corpus_layer"],
        ),
    ]

    for label, path, required_cols in specs:
        print(f"\nFile: {label}")

        if not path.exists():
            print(f"  [ERROR] FILE NOT FOUND: {path}")
            all_pass = False
            continue

        df = pd.read_csv(path)
        print(f"  Rows:    {len(df)}")
        print(f"  Columns: {list(df.columns)}")

        # Required columns
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"  [ERROR] MISSING REQUIRED COLUMNS: {missing}")
            all_pass = False
        else:
            print(f"  [OK] All required columns present")

        # Privacy check
        forbidden_found = [c for c in df.columns if c.lower() in FORBIDDEN_EXPORT_COLS]
        if forbidden_found:
            print(f"  [ERROR] PRIVACY VIOLATION - forbidden columns present: {forbidden_found}")
            all_pass = False
        else:
            print(f"  [OK] Privacy check passed - no forbidden columns")

        # Corpus layer distribution
        if "corpus_layer" in df.columns:
            print(f"  corpus_layer: {df['corpus_layer'].value_counts().to_dict()}")

        # Tertile distribution
        if "tertile" in df.columns:
            print(f"  tertile:      {df['tertile'].value_counts().sort_index().to_dict()}")

        # Community distribution
        if "subreddit_name" in df.columns:
            print(f"  communities:  {df['subreddit_name'].value_counts().to_dict()}")

        # deductive_codes spot check — confirm JSON parseable and non-empty
        if "deductive_codes" in df.columns:
            parseable = 0
            has_codes  = 0
            for val in df["deductive_codes"]:
                codes = _parse_json_field(val, [])
                if isinstance(codes, list):
                    parseable += 1
                    if codes:
                        has_codes += 1
            print(f"  deductive_codes: {parseable}/{len(df)} parseable, "
                  f"{has_codes}/{len(df)} non-empty")

        # Null post_id
        if "post_id" in df.columns:
            null_n = df["post_id"].isna().sum()
            status = f"[OK] None" if null_n == 0 else f"[WARN] {null_n} null post_ids"
            print(f"  null post_ids: {status}")

    print(f"\n{'='*65}")
    outcome = "[PASS] ready for module3_aggregation.py" if all_pass else \
              "[FAIL] review warnings above before running Module 3"
    print(f"RESULT: {outcome}")
    print(f"{'='*65}\n")

    return all_pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export coded SQLite data to CSV for module3_aggregation.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/export_coded_data.py
  python scripts/export_coded_data.py --db data/research_data.db --out outputs/
  python scripts/export_coded_data.py --dry-run
  python scripts/export_coded_data.py --validate
        """,
    )
    parser.add_argument(
        "--db", type=Path, default=DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_DIR,
        help=f"Output directory for CSV files (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Inspect DB schema only — no files written",
    )
    parser.add_argument(
        "--validate", action="store_true",
        help="Validate existing output CSVs without re-exporting",
    )
    args = parser.parse_args()

    coded_path    = args.out / "coded_posts.csv"
    emergent_path = args.out / "emergent_candidates.csv"

    if not args.db.exists():
        logger.error(
            "Database not found: %s\n"
            "Run bqah_coder.py --run first, or pass --db <path>.",
            args.db,
        )
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.create_function("chr", 1, chr)
    conn.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))

    try:
        if args.dry_run:
            dry_run_inspect(conn, args.db)
            return

        if args.validate:
            passed = validate_exports(coded_path, emergent_path)
            sys.exit(0 if passed else 1)

        # Full export
        coded_df = export_coded_posts(conn, coded_path)
        export_emergent_candidates(conn, emergent_path)

    except RuntimeError as e:
        logger.error("Export failed: %s", e)
        sys.exit(1)
    finally:
        conn.close()

    # Always validate after successful export
    passed = validate_exports(coded_path, emergent_path)

    if passed:
        print("Next steps:")
        print(f"  python -c \"")
        print(f"    from modules.module3_aggregation import run_all")
        print(f"    run_all('{coded_path}', '{emergent_path}', 'outputs/chapter4')\"")
        print()
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
