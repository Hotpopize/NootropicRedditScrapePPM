# Synthetic Sample Data

This directory contains **entirely fabricated** sample data for testing the
NootropicRedditScrapePPM analysis pipeline end-to-end without requiring any
live Reddit access and without using any real user content.

## What's in here

| File | Description |
|---|---|
| `synthetic_nootropics_sample.csv` | 28 hand-written synthetic posts exercising the Push-Pull-Mooring framework |
| `README.md` | This file |

## Provenance — every row is fabricated

Every post in `synthetic_nootropics_sample.csv` was written by the thesis
author (Vladislav Dolgov, Modul University Vienna) specifically for testing
this tool. **No text in this file is scraped, paraphrased, summarised, or
otherwise derived from any real Reddit post, comment, user, or community.**

Each row carries multiple explicit markers of its synthetic status:

- The first column of every row is `SYNTHETIC=TRUE`.
- Every `id` matches `synth_NNN` (e.g. `synth_001` .. `synth_028`).
- Every `author` matches `synth_user_NN` (e.g. `synth_user_01` .. `synth_user_28`).
- Every `permalink` and `url` points at the reserved `.example` top-level
  domain (RFC 2606), which cannot resolve to any real resource.
- Every `data_source` field is set to `synthetic_sample`.

If a reviewer, collaborator, or downstream tool ever finds a row that lacks
any of these markers, that row is a bug and should be treated as suspect.

## Why this file exists

The research analysis pipeline — the Push-Pull-Mooring (PPM) LLM coder, the
topic modeling module, the dashboard, and the codebook manager — must be
testable in three situations where real Reddit data is not available or not
appropriate:

1. **Continuous development.** Re-running the full pipeline against live
   Reddit data on every code change would burn API quota, slow iteration,
   and needlessly re-fetch content that has not changed.
2. **Third-party review.** Reviewers of this repository (including the
   Reddit Data API team, the thesis examining committee, and any future
   researchers evaluating whether to reuse this tool) must be able to
   clone the repo, run `python scripts/import_external_data.py`, and
   exercise the entire pipeline end-to-end — **without needing Reddit API
   credentials, and without touching a single byte of real Reddit content.**
3. **Reproducibility and open-source reuse.** Per §3 of the Reddit
   Research Data Addendum executed on 2026-04-01, the underlying code of
   this research must be made publicly available. A publicly-cloned repo
   with no runnable example is a worse open-source artifact than one with
   a self-contained demo dataset.

The synthetic sample answers all three needs without creating any of the
compliance problems that come with redistributing real Reddit data.

## Compliance note — this is not redistribution

§2 of the Reddit Research Data Addendum prohibits redistribution of Reddit
data and prohibits making Reddit data publicly available without Reddit's
written permission. This file is **not** Reddit data. It is original
fictional content written by the researcher for the sole purpose of
exercising the analysis pipeline. Committing this file to a public
repository does not implicate the redistribution clause.

If Reddit's review team disagrees with this interpretation, the file can be
removed from the repository and regenerated locally by any developer via
`python scripts/generate_mock_ppm_data.py` — the source-of-truth for every
post is the hand-written Python list in that script, not the committed CSV.

## Dataset structure

The CSV has 19 columns. The schema is flat (no nested JSON) so that any
reviewer can open the file in a spreadsheet tool and audit every row
without needing a JSON parser.

| Column | Description |
|---|---|
| `SYNTHETIC` | Always `TRUE`. Present as the first column so the synthetic status is visible before any content. |
| `id` | Deterministic synthetic id `synth_001` .. `synth_028`. |
| `type` | Always `submission` (no synthetic comments in this dataset). |
| `subreddit` | Target subreddit name (without the `r/` prefix). |
| `title` | Post title. |
| `text` | Post body / selftext. |
| `author` | Synthetic username `synth_user_01` .. `synth_user_28`. |
| `score` | Fabricated upvote count. |
| `num_comments` | Fabricated comment count. |
| `created_utc` | Unix timestamp. Deterministic — offsets of 25 days starting from 2023-01-01. |
| `created_date_iso` | Human-readable ISO date corresponding to `created_utc`. |
| `url` | Points at `https://reddit.example/...` (RFC 2606 reserved TLD). |
| `permalink` | Same as `url` — non-resolvable by design. |
| `data_source` | Always `synthetic_sample`. |
| `content_type` | Always `text`. |
| `language_flag` | Always `english`. |
| `word_count` | Computed from `text`. Range: 82–148 words. |
| `ppm_hint` | Ground-truth PPM category label. One of `push`, `pull`, `mooring`, `mixed`. |
| `collected_at` | ISO timestamp of when the script was run. This is the only non-deterministic column. |

## Push-Pull-Mooring distribution

The 28 posts were written to deliberately exercise all four branches of the
PPM coding taxonomy used by the thesis analysis pipeline. The `ppm_hint`
column in each row is a **weak ground-truth label** provided by the author
at write-time, suitable for evaluating the LLM coder's agreement with
human intuition on unambiguous cases. It is not a blinded gold standard
and should not be used for publication-grade validation.

| Category | Count | Rationale |
|---|---|---|
| `push` | 6 | Posts expressing dissatisfaction with incumbent options (prescription stimulants, caffeine, energy drinks). |
| `pull` | 9 | Posts expressing attraction to natural nootropic alternatives (lion's mane, rhodiola, bacopa, ashwagandha, omega-3, creatine, ginkgo, matcha, mushroom blends). |
| `mooring` | 10 | Posts expressing switching barriers — cost, habit, ritual, social norms, trust, dosing confusion, placebo doubt, physician dismissal, ethics, withdrawal fear, prior bad experience. |
| `mixed` | 3 | Longer posts that deliberately span multiple PPM categories, useful for stress-testing the coder on ambiguous cases. |

The slight overweight on `mooring` reflects the PPM literature's finding
that switching barriers dominate consumer narratives around habitual
products, and ensures the coder is stress-tested against the hardest
category to auto-classify.

## How to regenerate

```bash
python scripts/generate_mock_ppm_data.py
```

The generator is deterministic — re-running it produces byte-identical
output except for the `collected_at` column (which captures the generation
timestamp for provenance). To diff the content against a previous run,
exclude the `collected_at` column.

## How to use this file

See the main repository `README.md` section **"Quickstart with sample
data"** for instructions on loading this file into the local research
database via `scripts/import_external_data.py` and running the full PPM
analysis pipeline against it.
