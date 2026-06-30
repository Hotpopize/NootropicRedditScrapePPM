# BigQuery Extraction — Technical Search Parameters Reference

* **Project:** `NootropicRedditScrapePPM`
* **BQAH Session:** 2026-05-04
* **Researcher:** Vladislav Dolgov (MSc Management, Modul University Vienna)

This document serves as the official technical reference for the BigQuery Analytics Hub (BQAH) data extraction process used to construct the research corpus.

---

## 1. Session Environment

### Project & Datasets
* **Google Cloud Project:** `<REDDIT_PROJECT_ID>`
* **Production Dataset:** `for_researchers_external`
* **Validation (Mock) Dataset:** `mock_data_external`
* **Tables Scanned:** `posts`, `comments`, `subreddits`, `accounts`

### Date Parameters
Always declare these variables at the top of the session queries rather than hardcoding literals:
```sql
DECLARE start_date DATE DEFAULT '2020-01-01';
DECLARE end_date   DATE DEFAULT '2025-12-31';
```

### Cache Busting
Required on all real data runs to prevent stale or cached results:
```sql
DECLARE bust STRING DEFAULT CAST(CURRENT_TIMESTAMP() AS STRING);
```

---

## 2. Confirmed Subreddit IDs

Resolved and locked from `for_researchers_external.subreddits`. Do not re-query or modify these IDs.

* `'t5_2r81c'` — **r/Nootropics**
* `'t5_2qhb8'` — **r/Supplements**
* `'t5_2v89v'` — **r/Decaf**
* `'t5_2vnoe'` — **r/Biohackers**
* `'t5_4aoxhu'` — **r/NooTopics**
* `'t5_2ttk1'` — **r/StackAdvice**

### SQL Whitelist Pattern
Use in every query's `WHERE` clause:
```sql
p.subreddit_id IN (
    't5_2r81c', 't5_2qhb8', 't5_2v89v',
    't5_2vnoe', 't5_4aoxhu', 't5_2ttk1'
)
```

---

## 3. Quality Gate Parameters

### 3.1 Standard Gate (All communities except r/StackAdvice)
* `score >= 50`
* `body IS NOT NULL`
* `LENGTH(TRIM(body)) >= 250`

### 3.2 StackAdvice-Specific Gate (`t5_2ttk1` only)
* `score >= 25`
* `body IS NOT NULL`
* `LENGTH(TRIM(body)) >= 50`
* *Justification:* `NTILE(3)` analysis on production data confirmed score &ge; 25 is the upper tertile equivalent for `r/StackAdvice` (T1 range 34–72 vs. `r/Biohackers` T1 range 189–3,860).

### 3.3 Combined Quality Gate SQL Pattern
```sql
AND (
    (p.subreddit_id != 't5_2ttk1'
        AND p.score >= 50
        AND p.body IS NOT NULL
        AND LENGTH(TRIM(p.body)) >= 250)
    OR
    (p.subreddit_id = 't5_2ttk1'
        AND p.score >= 25
        AND p.body IS NOT NULL
        AND LENGTH(TRIM(p.body)) >= 50)
)
```

---

## 4. Layer A Keyword Filter

Keywords are applied to **both title and body** (case-insensitive `LOWER()` wrapper). There are six `OR` conditions total (3 groups &times; 2 fields).

### 4.1 Primary Switching Signals
```regex
switched from|quit caffeine|started nootropics|replacing adderall|moved to|instead of|alternative to|gave up
```

### 4.2 Push Factors (PUSH-01 to PUSH-07)
```regex
caffeine crash|jitters|anxiety from|tolerance|side effects|dependency|withdrawal|energy crash
```

### 4.3 Pull Factors (PULL-01 to PULL-07) — Substance-Specific
```regex
cognitive benefits|focus improvement|mental clarity|natural alternative|lion\'?s mane|rhodiola|bacopa|ashwagandha|omega[- ]?3|l[- ]?theanine|matcha|mushroom coffee|ginkgo|creatine
```

### 4.4 Mooring Signals (MOOR-I03, MOOR-I04, MOOR-F02, MOOR-I02)
```regex
is this safe|interaction|dosage|rate my stack|combine|negative interaction|how long|too much|tolerance break|cycling|am i taking|where to buy|is it worth
```

### 4.5 Keyword Design Notes
* **`\btea\b` Word Boundary:** Required during regex execution. Using `tea` as a simple substring produces false positives (e.g., matching "instead" due to "tea").
* **Retained Signals:** `"moved to"`, `"instead of"`, `"how long"`, and `"too much"` are retained despite false positive risk; qualitative LLM-coding filters them out at the code-assignment stage.
* **Incumbents Excluded:** Generic terms like `caffeine`, `coffee`, or `modafinil` were excluded from the keyword filter to prevent flooding (returned 118,499 posts / 16,217 post quality filter, which is incompatible with qualitative analysis).
* **Pull/Push Asymmetry (Retrieval-Stage Layer A Property):** Pull keywords are substance-specific, while push keywords are sentiment expressions (documented limitation).

---

## 4.6 Sampling and Quote Verification

* **Layer B (Inductive Emergent):** Data sampling that excludes Layer A post IDs to capture emergent supplements.
* **Quote Verification:** Fuzzy quote verification via RapidFuzz (90% similarity threshold) comparing model quotes against the raw post body. Unsupported codes are stripped.

---

## 5. NTILE(3) Stratification Parameters

### 5.1 Computation Order (Non-Negotiable)

#### Layer A (Keyword-Filtered)
1. Apply combined quality gate + keyword filter in `WHERE` clause.
2. Calculate `NTILE(3)` on the filtered pool only.
3. Apply `ROW_NUMBER()` within each tertile partition.
4. Cap at 20 posts per tertile per community.

#### Layer B (Inductive Emergent)
1. Apply combined quality gate (no keyword filter) in `WHERE` clause.
2. Exclude all Layer A post IDs *before* computing NTILE (critical).
3. Calculate `NTILE(3)` on the already-excluded pool.
4. Apply `ROW_NUMBER()` within each tertile partition.
5. Cap at 17/16/17 posts per tertile per community.

### 5.2 NTILE SQL Window Function
```sql
NTILE(3) OVER (
    PARTITION BY subreddit_id
    ORDER BY score DESC
) AS tertile
-- Tertile 1 = highest scoring third
-- Tertile 2 = middle scoring third
-- Tertile 3 = lowest scoring third
```

### 5.3 ROW_NUMBER with Deterministic Tie-Breaking
```sql
ROW_NUMBER() OVER (
    PARTITION BY subreddit_id, tertile
    ORDER BY score DESC, id ASC   -- id ASC breaks score ties deterministically
) AS rn
```

### 5.4 Sampling Caps SQL Pattern
```sql
-- Layer A
WHERE rn <= 20

-- Layer B (50 total per community: middle absorbs remainder)
WHERE (tertile = 1 AND rn <= 17)
   OR (tertile = 2 AND rn <= 16)
   OR (tertile = 3 AND rn <= 17)
```

---

## 6. Confirmed Tertile Ranges (Production Validation)

| Community | T1 Score Range | T2 Score Range | T3 Score Range | Pool Sizes (T1/T2/T3) |
|---|---|---|---|---|
| **r/Nootropics** | 140–1,091 | 77–140 | 50–77 | 165 / 165 / 165 |
| **r/Supplements** | 118–1,503 | 69–118 | 50–69 | 156 / 155 / 155 |
| **r/Decaf** | 86–1,364 | 63–85 | 50–63 | 116 / 116 / 116 |
| **r/Biohackers** | 189–3,860 | 84–188 | 50–83 | 111 / 111 / 110 |
| **r/NooTopics** | 112–1,130 | 65–111 | 50–62 | 24 / 24 / 23 |
| **r/StackAdvice** | 34–72 | 28–34 | 25–28 | 32 / 32 / 31 |

*Note:* `r/NooTopics` T3 has 23 posts total; all 23 are available and 20 are sampled.

---

## 7. Corpus Sampling Architecture

* **Layer A (Keyword-Filtered):** 6 communities &times; 3 tertiles &times; 20 posts = **360 posts**
* **Layer B (Emergent):** 6 communities &times; 50 posts (split 17/16/17) = **300 posts**
* **Total Corpus:** **660 posts** (110 per community)

### 7.1 Layer B Qualifying Pool Sizes (After Layer A Exclusions)
| Community | Qualifying Pool | Sampled | Buffer |
|---|---|---|---|
| **r/Biohackers** | 888 | 50 | 838 |
| **r/Supplements** | 676 | 50 | 626 |
| **r/Nootropics** | 469 | 50 | 419 |
| **r/Decaf** | 342 | 50 | 292 |
| **r/StackAdvice** | 57 | 50 | 7 *(minimal)* |
| **r/NooTopics** | 51 | 50 | 1 *(minimal)* |

---

## 8. Output Column Specifications

### 8.1 Posts Output Columns (15 Columns)
```sql
id,
subreddit_id,
CASE subreddit_id
    WHEN 't5_2r81c'  THEN 'Nootropics'
    WHEN 't5_2qhb8'  THEN 'Supplements'
    WHEN 't5_2v89v'  THEN 'Decaf'
    WHEN 't5_2vnoe'  THEN 'Biohackers'
    WHEN 't5_4aoxhu' THEN 'NooTopics'
    WHEN 't5_2ttk1'  THEN 'StackAdvice'
END AS subreddit_name,
title,
body,
score,
upvote_ratio,
flair_text,
DATE(created_at) AS post_date,
corpus_layer,     -- 'A_keyword' or 'B_emergent'
tertile,          -- 1, 2, or 3
nsfw,
locked,
permalink
```
* **EXCLUDED:** `author_id` (pre-hashed upstream, omitted in compliance with the Reddit Research Data Addendum).

### 8.2 Comments Output Columns (10 Columns)
```sql
id              AS comment_id,
post_id,
parent_id,       -- NULL for top-level comments
subreddit_id,
body,
score,
gilded,
permalink,
DATE(created_at) AS comment_date,
corpus_layer     -- inherited from parent post via JOIN
```
* **EXCLUDED:** `author_id` (compliance).
* **EXCLUDED:** `last_modified_at` (STRING type, sparse NaN, unusable).

---

## 9. Corpus Layer Values

* `'A_keyword'` &rarr; Layer A: keyword-filtered deductive posts.
* `'B_emergent'` &rarr; Layer B: inductive emergent posts.

---

## 10. Comment Extraction Parameters

* **Scope:** All comments associated with the 660 corpus post IDs.
* **Keywords:** No keyword filter is applied to comments (full thread capture).
* **Provenance:** `corpus_layer` inherited from parent post via `INNER JOIN`.

```sql
FROM for_researchers_external.comments c
INNER JOIN corpus_posts cp ON c.post_id = cp.id
WHERE c.subreddit_id IN (
    't5_2r81c', 't5_2qhb8', 't5_2v89v',
    't5_2vnoe', 't5_4aoxhu', 't5_2ttk1'
)
AND DATE(c.created_at) BETWEEN start_date AND end_date
```

### Mandatory Pre-Extraction COUNT Structure
```sql
SELECT
    COUNT(*)                                AS total_comments,
    COUNT(DISTINCT post_id)                 AS posts_with_comments,
    ROUND(COUNT(DISTINCT post_id)/660.0, 3) AS thread_coverage_rate
FROM for_researchers_external.comments
WHERE post_id IN (SELECT id FROM corpus_posts_cte)
  AND subreddit_id IN (
      't5_2r81c', 't5_2qhb8', 't5_2v89v',
      't5_2vnoe', 't5_4aoxhu', 't5_2ttk1'
  )
  AND DATE(created_at) BETWEEN start_date AND end_date;
```

---

## 11. Compliance Parameters (Enforced on Every Query)

* **`author_id` Protection:** NEVER include in `SELECT`, NEVER aggregate, NEVER join with external datasets.
* **`last_modified_at` Exclusions:** NEVER use for filtering, NEVER output (contains sparse NaN strings).
* **No Wildcards:** `SELECT *` is strictly forbidden. Always enumerate columns explicitly.
* **Mock Validation:** Always run queries on `mock_data_external` before moving to `for_researchers_external`.
* **Mock ID Handling:** Always use subreddit *name* joins on mock datasets because subreddit IDs differ from production.
* **Real ID Handling:** Always use confirmed `subreddit_id` whitelist values on real data.
* **Pre-Extraction COUNTS:** Always verify counts before exporting rows.
* **Data Minimization:** Only extract the columns and rows confirmed necessary for the analysis.

---

## 12. Known Limitations & Documented Anomalies (§3.5)

* **False-Positive Keywords:** General English phrases such as `"moved to"`, `"instead of"`, `"how long"`, and `"too much"` are retained. The local qualitative LLM coder filters out false matches at the code-assignment stage.
* **Incumbent Exclusion:** Substance terms like `caffeine` and `modafinil` were excluded from the keyword filter to prevent excessive returns. Layer B captures general incumbent discussions inductively.
* **Pull/Push Asymmetry (Retrieval-Stage Layer A Property):** Pull keywords are substance-specific, while push keywords are sentiment expressions.
* **Zero-Comment Posts:** 657/660 posts have comments (99.5% thread coverage). There are 3 posts with zero comments.
* **COUNT vs. Extraction Discrepancy:** The pre-extraction comments count was 57,780, and the raw extraction returned 72,737 comments due to duplicate rows created by independent NTILE materialization across separate jobs. These duplicate rows were collapsed and deduplicated during export, yielding the final, unique 57,755 comments imported.
* **OFFSET Boundary Deduplication:** 2 duplicate comment IDs at `t3_13rgerb` with score=0 ties were removed, resulting in 46,903 comments in the remaining-5 export.
* **Comment Extraction Cost:** `r/Nootropics` extraction alone billed 1.89 TB because the posts table was rescanned. This was resolved for the remaining 5 subreddits by using `UNNEST()` with hardcoded post IDs to prevent NTILE duplication.

---

## 13. Extraction Job Audit Log

All jobs ran on project `<REDDIT_PROJECT_ID>`, location `US`, by user `vladislav.dolgov@redditresearchers.com`.

| Step | Job ID | GB Billed | Rows | Status |
|---|---|---|---|---|
| **Mock Validation (Nootropics)** | `script_job_f46857ca2c2a3ca6f3518597cab799bb_0` | 9.49 | 0 (expected) | ✅ |
| **Real COUNT — posts** | `script_job_122953af61992e761bff4e8d64de2963_0` | 403.41 | 1 (summary) | ✅ 660 confirmed |
| **Real COUNT — comments** | `script_job_122953af61992e761bff4e8d64de2963_0` | 403.41 | 1 (summary) | ✅ 57,780 est. |
| **Post Extraction SELECT** | `script_job_7f1035af3aba002e91db0811f34b640e_0` | 247.22 | 660 | ✅ |
| **Comments — Nootropics** | `script_job_bf522b34ae9e2c25b87d9dc9631039f7_0` | 1,890.00 | 25,834 | ✅ |
| **Comments — remaining 5 (p1)** | `script_job_6232915ab3dcdea151f09fd3c282ecc6_0` | 1,680.00 | 27,609 | ✅ (browser cap) |
| **Comments — remaining 5 (p2)** | `script_job_34f5bd6a4cb41de6d279e6693c1608e5_0` | 1,680.00 | 19,294 | ✅ (OFFSET continuation) |

* **Total Billed Volume:** ~6,323 GB (~6.3 TB)

---

## 14. Final Corpus Summary

### Posts
* **Layer A (Keyword-Filtered):** 360 posts (60 per community &times; 6)
* **Layer B (Emergent):** 300 posts (50 per community &times; 6)
* **Total Posts:** **660** (110 per community &times; 6)

### Comments
* **Nootropics:** 25,834 comments
* **Supplements:** 15,309 comments
* **Biohackers:** 17,020 comments
* **Decaf:** 3,292 comments
* **NooTopics:** 7,713 comments
* **StackAdvice:** 3,570 comments
* **Total Unique Comments:** **72,737**

* **Thread Coverage:** 99.5% (657 / 660 posts have comments)
* **Date Range:** 2020-01-01 to 2025-12-31
* **Total Communities:** 6

### 14.1 Dataset Reconciliation

The count details for the BQAH raw data through the local analysis stages:

| Phase | Count | Delta | Description |
|---|---|---|---|
| **Raw Extracted Comments (BigQuery)** | **72,737** | — | Raw comments returned from BigQuery jobs (Nootropics: 25,834; remaining 5: 46,903). |
| **Imported BQAH Corpus (Disk)** | **57,755** | **-14,982** | Unique comments imported from BQAH CSV files after deduplication. |
| **Pre-LLM Filtered** | **52,448** | **-5,307** | Comments remaining after length (>40 chars) and language filtering. |
| **Certified Retained** | **49,435** | **-3,013** | Cleaned comments after text and quote verification filters. |

#### Submission (Posts) Reconciliation
* **Raw Extracted Submissions:** 660 posts
* **Imported/Coded Submissions:** 2,423 posts (includes 1,763 test posts)
* **Certified Clean Submissions:** 609 posts

---

## 15. Quick Reference for Future SQL Work

Paste this configuration block at the top of any new BQAH query:

```sql
-- DECLARE Date Range and Cache Bust
DECLARE start_date DATE DEFAULT '2020-01-01';
DECLARE end_date   DATE DEFAULT '2025-12-31';
DECLARE bust       STRING DEFAULT CAST(CURRENT_TIMESTAMP() AS STRING);

-- Subreddit Whitelist Whitelist reference:
-- 't5_2r81c'  r/Nootropics
-- 't5_2qhb8'  r/Supplements
-- 't5_2v89v'  r/Decaf
-- 't5_2vnoe'  r/Biohackers
-- 't5_4aoxhu' r/NooTopics
-- 't5_2ttk1'  r/StackAdvice

-- Standard Quality Gate:
-- score >= 50 AND body IS NOT NULL AND LENGTH(TRIM(body)) >= 250

-- StackAdvice Quality Gate:
-- score >= 25 AND body IS NOT NULL AND LENGTH(TRIM(body)) >= 50
```
