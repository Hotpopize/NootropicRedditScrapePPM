"""
Chapter 4 Aggregation Module — NootropicRedditScrapePPM
=======================================================
Produces the four Chapter 4 input tables from coded_posts.csv output.

Tables produced:
    4.1  community_ppm_profiles.csv     — community × PPM dimension matrix
    4.2  tertile_sensitivity.csv        — tertile × PPM sensitivity analysis
    4.3  emergent_code_candidates.csv   — emergent code catalogue (Layer B)
    4.4  code_frequency_heatmap.csv     — 6 communities × 24 codes heatmap

Usage (sample test — run before full corpus):
    from modules.module3_aggregation import run_all

    tables = run_all(
        coded_posts_path="coded_posts_sample.csv",
        emergent_candidates_path="emergent_candidates.csv",
        output_dir="outputs/chapter4_sample",
    )

Usage (full corpus):
    tables = run_all(
        coded_posts_path="coded_posts.csv",
        emergent_candidates_path="emergent_candidates.csv",
        output_dir="outputs/chapter4",
    )

BOUNDARY:
    This module produces quantitative inputs for researcher archetype
    analysis. It does NOT produce archetype labels or interpretive profiles.
    Archetype naming is researcher analytical judgment per §3.4 Stage 6.

PRIVACY:
    No post body text is written to any output table.
    No author_id field is read or propagated.
    Evidence quotes appear only in emergent_candidates input — not in output.
"""

import json
import logging
import os
import itertools
from typing import Optional

import pandas as pd
from rapidfuzz import process as fuzz_process
from rapidfuzz.fuzz import token_sort_ratio as rapid_token_sort_ratio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — PPM codebook (24 a priori codes per Metaprompt 3)
# ---------------------------------------------------------------------------

PUSH_CODES = {
    "PUSH-01", "PUSH-02", "PUSH-03", "PUSH-04",
    "PUSH-05", "PUSH-06", "PUSH-07",
}
PULL_CODES = {
    "PULL-01", "PULL-02", "PULL-03", "PULL-04",
    "PULL-05", "PULL-06", "PULL-07",
}
MOOR_F_CODES = {"MOOR-F-01", "MOOR-F-02", "MOOR-F-03", "MOOR-F-04"}
MOOR_I_CODES = {"MOOR-I-01", "MOOR-I-02", "MOOR-I-03", "MOOR-I-04", "MOOR-I-05", "MOOR-I-06"}

ALL_24_CODES = (
    sorted(PUSH_CODES) +
    sorted(PULL_CODES) +
    sorted(MOOR_F_CODES) +
    sorted(MOOR_I_CODES)
)

DIMENSION_MAP: dict[str, str] = {}
DIMENSION_MAP.update({c: "PUSH"   for c in PUSH_CODES})
DIMENSION_MAP.update({c: "PULL"   for c in PULL_CODES})
DIMENSION_MAP.update({c: "MOOR-F" for c in MOOR_F_CODES})
DIMENSION_MAP.update({c: "MOOR-I" for c in MOOR_I_CODES})
DIMENSION_MAP.update({
    "PUSH": "PUSH",
    "PULL": "PULL",
    "MOOR-F": "MOOR-F",
    "MOOR-I": "MOOR-I"
})

COMMUNITIES = [
    "Nootropics", "Supplements", "Decaf",
    "Biohackers", "NooTopics", "StackAdvice",
]

# Sensitivity analysis flag thresholds (confirmed in session)
PUSH_BIAS_THRESHOLD   = 0.20   # T1 PUSH rate > T3 PUSH rate + 20pp → §3.5 Addition 3
MOOR_LOWER_THRESHOLD  = 0.15   # T3 MOOR-I rate > T1 MOOR-I rate + 15pp → §3.5 Addition 7
LOW_CONF_THRESHOLD    = 0.30   # >30% of posts have all-LOW confidence codes
BODY_LENGTH_THRESHOLD = 150    # mean body_length < 150 chars → §3.5 Addition 5
EMERGENT_FREQ_MIN     = 3      # minimum recurrence for admission consideration
FUZZY_MATCH_THRESHOLD = 85     # similarity score for label deduplication (RapidFuzz)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _parse_codes(raw: object) -> list[dict]:
    """
    Parse deductive_codes field from coded_posts.csv.
    Accepts JSON string, list, or None/NaN — always returns list[dict].
    Each dict expected shape: {"code": "PUSH-01", "confidence": "HIGH", ...}
    Posts with no codes return [] — retained in denominator calculations.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, list):
        parsed = raw
    else:
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Could not parse deductive_codes: %r — treating as empty.", raw)
            return []
    
    if not isinstance(parsed, list):
        return []
    
    # Robustness filter: discard non-dict elements (e.g. malformed raw strings)
    return [c for c in parsed if isinstance(c, dict)]


def _explode_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode coded_posts DataFrame into one row per (post × code) pair.

    Input columns consumed: post_id, subreddit_name, corpus_layer,
                            tertile, body_length, deductive_codes
    Output columns added:   code, dimension, confidence

    Posts with zero codes are retained as a single row with
    code=None so they contribute to post-count denominators.
    """
    records = []
    required = {"post_id", "subreddit_name", "deductive_codes"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"_explode_codes: missing required columns: {missing}")

    for _, row in df.iterrows():
        codes = _parse_codes(row["deductive_codes"])
        base = {
            "post_id":        row["post_id"],
            "subreddit_name": row["subreddit_name"],
            "corpus_layer":   row.get("corpus_layer"),
            "tertile":        row.get("tertile"),
            "body_length":    row.get("body_length"),
        }
        if not codes:
            # Retain post with null code — counts in denominator, not in numerators
            records.append({**base, "code": None, "dimension": None, "confidence": None})
        else:
            for c in codes:
                code_val = c.get("code")
                records.append({
                    **base,
                    "code":       code_val,
                    "dimension":  DIMENSION_MAP.get(code_val),
                    "confidence": c.get("confidence"),
                })

    exploded = pd.DataFrame(records)
    logger.debug("_explode_codes: %d posts → %d rows after explode", len(df), len(exploded))
    return exploded


def _all_low_confidence(codes: list[dict]) -> bool:
    """
    Return True if a post has ≥1 code AND all codes are LOW confidence.
    Posts with no codes return False — they are not 'all LOW', they are uncoded.
    Used for FLAG_LOW_CONFIDENCE calculation.
    """
    if not codes:
        return False
    return all(c.get("confidence") == "LOW" for c in codes)


def _dim_post_rate(group_exp: pd.DataFrame, dimension: str, total_posts: int) -> float:
    """
    Proportion of total_posts in group that carry ≥1 code in given dimension.
    Denominator is passed explicitly to ensure consistent normalisation.
    Returns 0.0 if total_posts is zero.
    """
    if total_posts == 0:
        return 0.0
    n = int(group_exp[group_exp["dimension"] == dimension]["post_id"].nunique())
    return n / total_posts


# ---------------------------------------------------------------------------
# Table 4.1 — Community × PPM Dimension Frequency Matrix
# ---------------------------------------------------------------------------

def build_table_4_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces community_ppm_profiles.csv.

    Each row = one community (6 rows total).
    Columns:
        subreddit_name, total_coded_units,
        PUSH_count, PUSH_pct,
        PULL_count, PULL_pct,
        MOOR_F_count, MOOR_F_pct,
        MOOR_I_count, MOOR_I_pct,
        MOOR_count, MOOR_pct,
        NONE_count, NONE_pct,
        dominant_dimension,
        code_PUSH-01_count ... code_MOOR-I-06_count  (24 supplementary columns)

    Count = posts carrying ≥1 code in that dimension (binary post-level).
    Pct   = formatted string: "count (pct%)" (e.g. "1197 (39.2%)").
            If count < 10, flagged as unstable with '*' (e.g. "9 (0.3%)*").
    NONE  = posts carrying 0 deductive PPM codes.
    MOOR  = posts carrying ≥1 Mooring code (MOOR-F OR MOOR-I) — combined mooring intensity.
    """
    exploded = _explode_codes(df)
    rows = []

    for community in COMMUNITIES:
        comm = exploded[exploded["subreddit_name"] == community]
        if comm.empty:
            logger.warning("Table 4.1: no data for community '%s' — skipping.", community)
            continue

        total = comm["post_id"].nunique()

        def dim_count(dim: str) -> int:
            return int(comm[comm["dimension"] == dim]["post_id"].nunique())

        push_n   = dim_count("PUSH")
        pull_n   = dim_count("PULL")
        moor_f_n = dim_count("MOOR-F")
        moor_i_n = dim_count("MOOR-I")
        
        # Combined mooring intensity (MOOR-F OR MOOR-I)
        moor_n = int(comm[comm["dimension"].isin(["MOOR-F", "MOOR-I"])]["post_id"].nunique())
        
        # NONE count = total - posts carrying any of the 4 dimensions
        any_dim_posts = comm[comm["dimension"].notna()]["post_id"].nunique()
        none_n = total - any_dim_posts

        def format_cell(n: int) -> str:
            pct_val = (n / total * 100) if total else 0.0
            s = f"{n} ({pct_val:.1f}%)"
            if n < 10:
                s += "*"
            return s

        dim_counts = {
            "PUSH": push_n, "PULL": pull_n,
            "MOOR-F": moor_f_n, "MOOR-I": moor_i_n,
        }
        dominant = max(dim_counts, key=dim_counts.get) if any(dim_counts.values()) else "N/A"

        row = {
            "subreddit_name":     community,
            "total_coded_units":  total,
            "PUSH_count":         push_n,
            "PUSH_pct":           format_cell(push_n),
            "PULL_count":         pull_n,
            "PULL_pct":           format_cell(pull_n),
            "MOOR_F_count":       moor_f_n,
            "MOOR_F_pct":         format_cell(moor_f_n),
            "MOOR_I_count":       moor_i_n,
            "MOOR_I_pct":         format_cell(moor_i_n),
            "MOOR_count":         moor_n,
            "MOOR_pct":           format_cell(moor_n),
            "NONE_count":         none_n,
            "NONE_pct":           format_cell(none_n),
            "dominant_dimension": dominant,
        }

        # Supplementary: individual code frequencies (24 columns)
        for code in ALL_24_CODES:
            row[f"code_{code}_count"] = int(
                comm[comm["code"] == code]["post_id"].nunique()
            )

        rows.append(row)

    result = pd.DataFrame(rows).reset_index(drop=True)
    logger.info(
        "Table 4.1 built — %d communities, %d columns.",
        len(result), len(result.columns),
    )
    return result


# ---------------------------------------------------------------------------
# Table 4.2 — Tertile Sensitivity Analysis
# ---------------------------------------------------------------------------

def build_table_4_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces tertile_sensitivity.csv.

    Each row = one community × tertile combination (up to 18 rows).
    Columns:
        subreddit_name, tertile, total_coded_units,
        PUSH_count, PUSH_pct,
        PULL_count, PULL_pct,
        MOOR_F_count, MOOR_F_pct,
        MOOR_I_count, MOOR_I_pct,
        MOOR_count, MOOR_pct,
        NONE_count, NONE_pct,
        FLAG_PUSH_BIAS, FLAG_MOOR_LOWER,
        FLAG_LOW_CONFIDENCE, FLAG_BODY_LENGTH

    FLAG definitions:
      FLAG_PUSH_BIAS: T1 PUSH rate > T3 PUSH rate + 0.20
      FLAG_MOOR_LOWER: T3 MOOR-I rate > T1 MOOR-I rate + 0.15
      FLAG_LOW_CONFIDENCE: (posts with all-LOW confidence / total coded posts) > 0.30
      FLAG_BODY_LENGTH: mean(body_length) < 150 AND FLAG_LOW_CONFIDENCE is True
    """
    exploded = _explode_codes(df)

    # Pre-compute all-LOW flag per post from original df
    df = df.copy()
    df["_all_low"] = df["deductive_codes"].apply(
        lambda x: _all_low_confidence(_parse_codes(x))
    )

    # First pass — build per-cell rates for cross-tertile flag computation
    cell_rates: dict[tuple[str, int], dict] = {}

    rows = []
    for community in COMMUNITIES:
        comm_exp = exploded[exploded["subreddit_name"] == community]
        comm_raw = df[df["subreddit_name"] == community]

        for tertile in [1, 2, 3]:
            t_exp = comm_exp[comm_exp["tertile"] == tertile]
            t_raw = comm_raw[comm_raw["tertile"] == tertile]

            total = t_exp["post_id"].nunique()

            push_rate   = _dim_post_rate(t_exp, "PUSH",   total)
            moor_i_rate = _dim_post_rate(t_exp, "MOOR-I", total)
            cell_rates[(community, tertile)] = {
                "push_rate":   push_rate,
                "moor_i_rate": moor_i_rate,
            }

            if total == 0:
                logger.warning(
                    "Table 4.2: no coded posts for %s tertile %d — row skipped.",
                    community, tertile,
                )
                continue

            def dim_count(dim: str) -> int:
                return int(t_exp[t_exp["dimension"] == dim]["post_id"].nunique())

            push_n   = dim_count("PUSH")
            pull_n   = dim_count("PULL")
            moor_f_n = dim_count("MOOR-F")
            moor_i_n = dim_count("MOOR-I")
            
            moor_n = int(t_exp[t_exp["dimension"].isin(["MOOR-F", "MOOR-I"])]["post_id"].nunique())
            
            any_dim_posts = t_exp[t_exp["dimension"].notna()]["post_id"].nunique()
            none_n = total - any_dim_posts

            def format_cell(n: int) -> str:
                pct_val = (n / total * 100) if total else 0.0
                s = f"{n} ({pct_val:.1f}%)"
                if n < 10:
                    s += "*"
                return s

            # Row-level flags
            all_low_n   = int(t_raw["_all_low"].sum())
            low_conf    = (all_low_n / total) > LOW_CONF_THRESHOLD if total else False
            mean_body   = (
                t_raw["body_length"].mean()
                if "body_length" in t_raw.columns and not t_raw["body_length"].isna().all()
                else 0.0
            )
            body_flag   = (mean_body < BODY_LENGTH_THRESHOLD) and low_conf

            rows.append({
                "subreddit_name":      community,
                "tertile":             tertile,
                "total_coded_units":   total,
                "PUSH_count":          push_n,
                "PUSH_pct":            format_cell(push_n),
                "PULL_count":          pull_n,
                "PULL_pct":            format_cell(pull_n),
                "MOOR_F_count":        moor_f_n,
                "MOOR_F_pct":          format_cell(moor_f_n),
                "MOOR_I_count":        moor_i_n,
                "MOOR_I_pct":          format_cell(moor_i_n),
                "MOOR_count":          moor_n,
                "MOOR_pct":            format_cell(moor_n),
                "NONE_count":          none_n,
                "NONE_pct":            format_cell(none_n),
                "FLAG_LOW_CONFIDENCE": low_conf,
                "FLAG_BODY_LENGTH":    body_flag,
            })

    result = pd.DataFrame(rows).reset_index(drop=True)

    # Second pass — compute community-level cross-tertile flags
    push_bias_col  = []
    moor_lower_col = []

    for _, row in result.iterrows():
        community = row["subreddit_name"]
        t1 = cell_rates.get((community, 1), {})
        t3 = cell_rates.get((community, 3), {})

        t1_push    = t1.get("push_rate",   0.0)
        t3_push    = t3.get("push_rate",   0.0)
        t1_moor_i  = t1.get("moor_i_rate", 0.0)
        t3_moor_i  = t3.get("moor_i_rate", 0.0)

        push_bias_col.append(
            bool(t1_push > t3_push + PUSH_BIAS_THRESHOLD)
        )
        moor_lower_col.append(
            bool(t3_moor_i > t1_moor_i + MOOR_LOWER_THRESHOLD)
        )

    result["FLAG_PUSH_BIAS"]  = push_bias_col
    result["FLAG_MOOR_LOWER"] = moor_lower_col

    # Reorder columns for readability
    result = result[[
        "subreddit_name", "tertile", "total_coded_units",
        "PUSH_count", "PUSH_pct",
        "PULL_count", "PULL_pct",
        "MOOR_F_count", "MOOR_F_pct",
        "MOOR_I_count", "MOOR_I_pct",
        "MOOR_count", "MOOR_pct",
        "NONE_count", "NONE_pct",
        "FLAG_PUSH_BIAS", "FLAG_MOOR_LOWER",
        "FLAG_LOW_CONFIDENCE", "FLAG_BODY_LENGTH",
    ]]

    logger.info(
        "Table 4.2 built — %d rows. "
        "FLAG_PUSH_BIAS fired: %d communities. "
        "FLAG_MOOR_LOWER fired: %d communities.",
        len(result),
        result.groupby("subreddit_name")["FLAG_PUSH_BIAS"].any().sum(),
        result.groupby("subreddit_name")["FLAG_MOOR_LOWER"].any().sum(),
    )
    return result


# ---------------------------------------------------------------------------
# Table 4.3 — Emergent Code Catalogue
# ---------------------------------------------------------------------------

def build_table_4_3(emergent_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces emergent_code_candidates.csv from Layer B emergent candidates.

    Input columns expected (from Module 2 emergent_candidates output):
        label, definition, partial_overlap_with, distinct_dimension,
        evidence_quote, confidence, subreddit_name, source_post_id

    Output columns:
        canonical_label           — deduplicated label via fuzzy matching
        definition                — first-seen definition for the canonical group
        frequency                 — total instances across all communities
        unique_posts_count        — count of unique posts carrying this theme
        communities_of_origin     — comma-separated list of source communities
        partial_overlap_with      — existing code that partially maps (if any)
        distinct_dimension        — what makes this distinct from that code
        exemplar_quote            — a representative quote from the group
        status                    — PENDING (researcher updates to ADMITTED/REJECTED)

    Fuzzy deduplication: RapidFuzz token_sort_ratio, threshold = 85.
    Labels scoring ≥ 85 against an existing canonical are merged into that group.
    Labels scoring < 85 become new canonicals.
    Purges false positives: remove degenerate, single-post themes (unique posts count < 2).
    """
    _EMPTY_COLS = [
        "canonical_label", "definition", "frequency", "unique_posts_count", "communities_of_origin",
        "partial_overlap_with", "distinct_dimension", "exemplar_quote", "status",
    ]

    if emergent_df is None or emergent_df.empty:
        logger.warning("Table 4.3: empty input — returning empty table.")
        return pd.DataFrame(columns=_EMPTY_COLS)

    if "label" not in emergent_df.columns:
        raise ValueError("Table 4.3: 'label' column required in emergent_df.")

    raw_labels = emergent_df["label"].dropna().tolist()
    if not raw_labels:
        logger.warning("Table 4.3: no labels found — returning empty table.")
        return pd.DataFrame(columns=_EMPTY_COLS)

    # Fuzzy deduplication — map each raw label to a canonical using length-blocked rapidfuzz
    canonical_map: dict[str, str] = {}
    canonicals_by_len: dict[int, list[str]] = {}

    for label in raw_labels:
        len_l = len(label)
        # Minimum possible length ratio to get score >= 85 is 0.74
        min_len = int(len_l * 0.74)
        max_len = int(len_l / 0.74) + 1
        candidates = []
        for l_key in range(min_len, max_len + 1):
            if l_key in canonicals_by_len:
                candidates.extend(canonicals_by_len[l_key])

        if not candidates:
            canonicals_by_len.setdefault(len_l, []).append(label)
            canonical_map[label] = label
            continue

        res = fuzz_process.extractOne(label, candidates, scorer=rapid_token_sort_ratio)
        if res and res[1] >= FUZZY_MATCH_THRESHOLD:
            canonical_map[label] = res[0]
            logger.debug(
                "Fuzzy merge: '%s' → '%s' (score=%d)", label, res[0], res[1]
            )
        else:
            canonicals_by_len.setdefault(len_l, []).append(label)
            canonical_map[label] = label

    working = emergent_df.copy()
    working["canonical_label"] = working["label"].map(canonical_map)

    rows = []
    for canonical, group in working.groupby("canonical_label"):
        # Count unique posts in this theme
        unique_posts = group["source_post_id"].dropna().nunique() if "source_post_id" in group.columns else group["post_id"].dropna().nunique() if "post_id" in group.columns else 0
        
        # Purge single-post themes (unique_posts < 2)
        if unique_posts < 2:
            continue

        frequency    = len(group)
        communities  = sorted(group["subreddit_name"].dropna().unique().tolist())
        
        # Find first non-empty definition and quote
        first_def = ""
        for d in group["definition"]:
            if pd.notna(d) and str(d).strip():
                first_def = str(d).strip()
                break

        exemplar_q = ""
        for q in group.get("evidence_quote", []):
            if pd.notna(q) and str(q).strip():
                exemplar_q = str(q).strip()
                break

        first = group.iloc[0]

        rows.append({
            "canonical_label":                    canonical,
            "definition":                         first_def or first.get("definition", ""),
            "frequency":                          frequency,
            "unique_posts_count":                 unique_posts,
            "communities_of_origin":              ", ".join(communities),
            "partial_overlap_with":               first.get("partial_overlap_with", ""),
            "distinct_dimension":                 first.get("distinct_dimension", ""),
            "exemplar_quote":                     exemplar_q,
            "status":                             "PENDING",
        })

    result = (
        pd.DataFrame(rows)
        .sort_values("frequency", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(
        "Table 4.3 built — %d emergent candidates after purging single-post themes.",
        len(result),
    )
    return result


# ---------------------------------------------------------------------------
# Table 4.1b — Dimension Co-occurrence
# ---------------------------------------------------------------------------

def build_cooccurrence_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a per-community dimension co-occurrence table.
    Counts the 16 combinations of co-present dimensions (PUSH, PULL, MOOR-F, MOOR-I).
    """
    records = []
    for _, row in df.iterrows():
        post_id = row["post_id"]
        subreddit = row["subreddit_name"]
        codes = _parse_codes(row["deductive_codes"])
        
        has_push = any(c.get("code") in PUSH_CODES for c in codes)
        has_pull = any(c.get("code") in PULL_CODES for c in codes)
        has_moorf = any(c.get("code") in MOOR_F_CODES for c in codes)
        has_moori = any(c.get("code") in MOOR_I_CODES for c in codes)
        
        records.append({
            "post_id": post_id,
            "subreddit_name": subreddit,
            "PUSH": has_push,
            "PULL": has_pull,
            "MOOR-F": has_moorf,
            "MOOR-I": has_moori,
        })
    
    posts_df = pd.DataFrame(records)
    dimensions = ["PUSH", "PULL", "MOOR-F", "MOOR-I"]
    
    def get_combo_name(r) -> str:
        active = [d for d in dimensions if r[d]]
        return "+".join(active) if active else "NONE"
        
    posts_df["combination"] = posts_df.apply(get_combo_name, axis=1)
    
    all_combos = []
    for r in range(1, len(dimensions) + 1):
        for combo in itertools.combinations(dimensions, r):
            all_combos.append("+".join(combo))
    all_combos.append("NONE")
    
    # Pivot table: index = combination, columns = subreddit_name
    pivot_df = posts_df.pivot_table(
        index="combination",
        columns="subreddit_name",
        values="post_id",
        aggfunc="count",
        fill_value=0
    )
    
    # Ensure all 6 communities are represented
    for comm in COMMUNITIES:
        if comm not in pivot_df.columns:
            pivot_df[comm] = 0
            
    pivot_df = pivot_df.reindex(all_combos, fill_value=0)
    pivot_df = pivot_df[COMMUNITIES] # Keep stable order of columns
    pivot_df["Total"] = pivot_df.sum(axis=1)
    
    return pivot_df


# ---------------------------------------------------------------------------
# Table 4.1c — Subcode Frequencies
# ---------------------------------------------------------------------------

def build_subcode_frequencies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compile and save corpus-wide individual subcode frequencies.
    """
    exploded = _explode_codes(df)
    coded = exploded[exploded["code"].notna()]
    total_posts = df["post_id"].nunique()
    
    rows = []
    for code in ALL_24_CODES:
        count = int(coded[coded["code"] == code]["post_id"].nunique())
        pct = (count / total_posts * 100) if total_posts else 0.0
        rows.append({
            "subcode": code,
            "dimension": DIMENSION_MAP.get(code),
            "count": count,
            "percentage": f"{pct:.2f}%"
        })
        
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Table 4.4 — Individual Code Frequency Heatmap
# ---------------------------------------------------------------------------

def build_table_4_4(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces code_frequency_heatmap.csv.

    Wide-format matrix:
        Index:   subreddit_name (6 communities)
        Columns: all 24 a priori codes ordered PUSH → PULL → MOOR-F → MOOR-I

    Cell value: number of coded posts in that community carrying that specific
    code (post count, not code instance count).

    Enables cross-community comparison at individual code level for P4
    archetype differentiation — e.g. r/Decaf dominated by PUSH-03
    (Dependency/Withdrawal) vs r/Biohackers dominated by PULL-04
    (Holistic Integration).

    NOTE: Zero-filled for communities with no posts carrying a given code.
    """
    exploded = _explode_codes(df)
    coded    = exploded[exploded["code"].notna()]

    rows = []
    for community in COMMUNITIES:
        comm = coded[coded["subreddit_name"] == community]
        row  = {"subreddit_name": community}
        for code in ALL_24_CODES:
            row[code] = int(
                comm[comm["code"] == code]["post_id"].nunique()
            )
        rows.append(row)

    result = (
        pd.DataFrame(rows)
        .set_index("subreddit_name")
        .reindex(columns=ALL_24_CODES, fill_value=0)
    )

    logger.info(
        "Table 4.4 built — %d communities × %d codes.",
        len(result), len(result.columns),
    )
    return result


# ---------------------------------------------------------------------------
# Runner — test on sample or full corpus
# ---------------------------------------------------------------------------

def run_all(
    coded_posts_path:          str,
    emergent_candidates_path:  str,
    output_dir:                str = "outputs/chapter4",
    sample_n:                  Optional[int] = None,
    post_type:                 str = "full_corpus",
) -> dict[str, pd.DataFrame]:
    """
    Build all Chapter 4 tables and save to output_dir.

    Args:
        coded_posts_path:         path to coded_posts.csv (Module 2 output)
        emergent_candidates_path: path to emergent_candidates.csv (Layer B only)
        output_dir:               directory for output CSVs (created if absent)
        sample_n:                 if set, run on first N posts only (for testing)
        post_type:                "full_corpus" or "submissions_only"

    Returns:
        dict mapping table name → DataFrame, for interactive inspection.
    """
    os.makedirs(output_dir, exist_ok=True)

    logger.info(
        "run_all: loading coded_posts from '%s'%s (post_type=%s)",
        coded_posts_path,
        f" (sample_n={sample_n})" if sample_n else "",
        post_type,
    )
    df = pd.read_csv(coded_posts_path)
    initial_len = len(df)
    
    # Filter out quarantined rows if present in the export
    if "quarantine_reason" in df.columns:
        df = df[df["quarantine_reason"].isna()]
        
    # Only include Llama models (excluding system_filter, researcher, etc.), machine_partition=None, and test posts
    df = df[
        df["model"].str.startswith("llama", na=False) &
        df["machine_partition"].notna() &
        (df["machine_partition"] != "None") &
        (df["machine_partition"] != "") &
        (df["data_source"].fillna("") != "json_endpoint")
    ]
    
    # Apply post_type filter
    if post_type == "submissions_only":
        df = df[df["post_id"].str.startswith("t3_", na=False)]
    elif post_type == "full_corpus":
        n_subs = df["post_id"].str.startswith("t3_", na=False).sum()
        n_comms = df["post_id"].str.startswith("t1_", na=False).sum()
        logger.info("run_all: split is %d submissions and %d comments.", n_subs, n_comms)
        
    filtered_len = len(df)
    logger.info(
        "run_all: filtered count. Initial: %d, Remaining: %d (%d excluded)",
        initial_len, filtered_len, initial_len - filtered_len
    )
    if sample_n:
        df = df.head(sample_n)
        logger.info("run_all: using sample of %d posts.", len(df))

    emergent_raw = pd.read_csv(emergent_candidates_path)

    # Layer B emergent candidates only
    if "corpus_layer" in emergent_raw.columns:
        emergent_b = emergent_raw[emergent_raw["corpus_layer"] == "B_emergent"].copy()
    else:
        emergent_b = emergent_raw

    # Apply post_type filter to emergent candidates as well
    if post_type == "submissions_only" and "source_post_id" in emergent_b.columns:
        emergent_b = emergent_b[emergent_b["source_post_id"].str.startswith("t3_", na=False)].copy()
        logger.info("run_all: filtered emergent candidates to submissions-only (%d rows)", len(emergent_b))

    tables = {
        "4_1_community_ppm_profiles":  build_table_4_1(df),
        "4_1b_dimension_cooccurrence": build_cooccurrence_table(df),
        "4_1c_subcode_frequencies":   build_subcode_frequencies(df),
        "4_2_tertile_sensitivity":     build_table_4_2(df),
        "4_3_emergent_code_candidates":build_table_4_3(emergent_b),
        "4_4_code_frequency_heatmap":  build_table_4_4(df),
    }

    for name, table in tables.items():
        path = os.path.join(output_dir, f"{name}.csv")
        # 4_4 heatmap and 4_1b cooccurrence write with index
        write_index = (name in ("4_4_code_frequency_heatmap", "4_1b_dimension_cooccurrence"))
        table.to_csv(path, index=write_index)
        logger.info("Saved %-40s → %s (%d rows)", name, path, len(table))

    logger.info("run_all complete. All tables written to '%s'.", output_dir)
    return tables


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--coded_posts", default="outputs/coded_posts.csv")
    ap.add_argument("--emergent_candidates", default="outputs/emergent_candidates.csv")
    ap.add_argument("--output_root", default="outputs/chapter4")
    args = ap.parse_args()
    
    # Run 1: Submissions-only
    print("Running aggregation for Submissions Only...")
    run_all(
        coded_posts_path=args.coded_posts,
        emergent_candidates_path=args.emergent_candidates,
        output_dir=os.path.join(args.output_root, "submissions_only"),
        post_type="submissions_only"
    )
    
    # Run 2: Full corpus
    print("Running aggregation for Full Corpus...")
    run_all(
        coded_posts_path=args.coded_posts,
        emergent_candidates_path=args.emergent_candidates,
        output_dir=os.path.join(args.output_root, "full_corpus"),
        post_type="full_corpus"
    )
