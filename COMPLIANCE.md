# Compliance Map — Reddit Research Data Addendum

This document maps every substantive clause of the Reddit Research Data
Addendum (executed 2026-04-01) to the specific code, documentation, policy,
or operational control in this repository that operationalises it. It
exists so that any reviewer — whether from Reddit, the Modul University
Vienna examining committee, or a future researcher considering reuse of
this tool — can audit compliance in under two minutes without having to
read the full source tree.

If anything in this document ever falls out of sync with the actual code,
the code is authoritative and this document is a bug — please file an
issue.

---

## Researcher Identity

| Field | Value |
|---|---|
| **Researcher** | Vladislav Dolgov |
| **Reddit account** | [u/MscNooManMuAt](https://www.reddit.com/user/MscNooManMuAt) |
| **Research contact** | 1821019@modul.ac.at |
| **Institution** | Modul University Vienna |
| **Programme** | MSc Management / MBA (joint) |
| **Supervisor** | Dr. Lyndon Nixon |
| **Reddit app** | `NootropicRedditScrapePPM` (script type, owned by u/MscNooManMuAt) |
| **User-Agent string** | `script:NootropicRedditScrapePPM:v1.0 (by /u/MscNooManMuAt)` |
| **Agreement** | Reddit Research Data Addendum (08192024), executed 2026-04-01 |
| **Reddit signatory** | Andrea Middleton, Director, Community Empowerment |
| **Researcher signatory** | Vladislav Dolgov, Modul University Vienna |
| **DocuSign envelope** | `0890DE2B-DB3B-4CBD-98F3-4A9B5C1790CA` |
| **Researcher support ticket** | `[17VP91-EZDNE]` |

The email on the signed addendum, the email on the Reddit developer
account, the Reddit username embedded in the User-Agent string, and the
ownership of the Reddit app registration are all consistent and resolve to
the same identity.

---

## Executive summary

This tool collects Reddit posts from a small, fixed set of topic-relevant
subreddits via the official authenticated Reddit API (PRAW), stores them
locally for the duration of an MSc/MBA thesis, and applies a local
Large-Language-Model-assisted qualitative coding pipeline using the
Push-Pull-Mooring (PPM) framework. The research question is aggregate and
discursive: *how do consumers talk about switching between caffeine,
prescription stimulants, and natural nootropics?* It is not clinical, not
commercial, not individual-level, and not profile-building. The analysis
unit is the post, not the user.

Four structural guarantees underpin the compliance posture below:

1. **Authenticated access is the only access path.** The unauthenticated
   JSON-endpoint scraper that was present in an earlier revision of the
   repository was deleted in commit `0182153`. The Streamlit application
   refuses to start unless valid Reddit API credentials authenticate
   successfully at launch.
2. **No real Reddit data is redistributed.** The repository contains no
   committed Reddit content of any kind. The only sample data shipped in
   the repository is an entirely fabricated synthetic corpus written by
   the researcher for testing, stored under `samples/` with a `SYNTHETIC`
   column flag on every row.
3. **PII never enters the analysis pipeline.** The `CollectedItem` schema
   enforces an `author` constraint at ingestion time that rejects raw
   usernames; only `[deleted]`, `[removed]`, prefixed pseudonyms, or
   SHA-256 hashes are accepted.
4. **Deletion compliance is automated.** `scripts/scrub_deleted_data.py`
   cross-references every locally-stored item against the live Reddit API
   and purges anything that has been removed or deleted upstream, in
   accordance with §2 of the addendum.

---

## Clause-by-clause compliance map

### §1 — Registration and Access

> *"You should only access and use the Researcher Services through tokens,
> keys, passwords, login credentials, and other access controls that are
> authorized and made available to you by Reddit… You may not share your
> Access Info or any data or information accessed with any other party
> without Reddit's express permission, and you should keep your Access
> Info and all Reddit data received secure at all times."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Access only via authorised credentials | Authenticated PRAW (OAuth2 via Reddit's `script` app type) is the single and only access path. The previous unauthenticated public-JSON-endpoint path has been removed from the codebase and from git history of `main` going forward. | `services/reddit_service.py` (PRAW client), deletion of `services/reddit_json_service.py` in commit `0182153` |
| Credentials not shared | Credentials live only in a local `.env` file that is excluded from version control via `.gitignore`. The `.env.example` template contains only placeholders. The Reddit app at `reddit.com/prefs/apps` lists only `u/MscNooManMuAt` as a developer. | `.gitignore` (line: `.env`), `.env.example`, Reddit app registration |
| Hard-fail startup gate | The Streamlit application calls `_verify_praw_credentials()` on every launch. If credentials are missing, malformed, or fail to authenticate against Reddit's API, the entire UI is blocked behind a title-screen error state and no collection code paths are reachable. | `app.py` (lines 66–100) |
| Descriptive User-Agent | A single canonical UA string is used across all API calls, in the Reddit-documented format `<platform>:<app ID>:<version> (by /u/<username>)`: `script:NootropicRedditScrapePPM:v1.0 (by /u/MscNooManMuAt)`. This string is identifiable in Reddit's API logs and tied to the account on the signed addendum. This document is the authoritative source for the UA string; `.env.example` and `services/reddit_service.py` track it. | This document (authoritative), `services/reddit_service.py`, `.env.example` |
| Accurate researcher info | The email on the signed addendum (`1821019@modul.ac.at`), the Reddit account email, the User-Agent Reddit username, and the repository commit author identity are all consistent. | Identity table above |
| Local data stored securely | All collected data is held in a local SQLite database outside of version control. The `data/` directory is gitignored. No committed artifact in this repository contains any Reddit user content. | `.gitignore` (lines: `*.db`, `data/`, `exports/`) |

### §2 — Research and Data Ethics

#### §2.a — Respect Redditors and their communities

> *"Do NOT abuse your access to Reddit data… Your research must comply
> with our Terms and have an academic and non-commercial purpose."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Academic purpose | This tool exists solely to support a single MSc/MBA thesis at Modul University Vienna, supervised by Dr. Lyndon Nixon. The research question is framed as aggregate consumer discourse, not clinical or individual. | `docs/research_scope.md` *(forthcoming — see "Related documents" below)*, thesis abstract on file with Reddit for Researchers |
| Non-commercial purpose | The repository is licensed under the PolyForm Noncommercial License 1.0.0, which prohibits commercial use of the code. No commercial product, API, service, or derivative is offered, planned, or foreshadowed in any part of this repository. | `LICENSE` |
| Rate-limit discipline | All API calls flow through a token-bucket `RateLimiter` configured well below Reddit's authenticated rate limits. Collection volume is bounded by a hard page cap and a thesis-level target of roughly 150–200 posts total. | `services/reddit_service.py` (RateLimiter), `core/schemas.py` (`RateLimitConfig`) |

#### §2.b — Respect the privacy of Redditors: identification and targeting

> *"You will not… use your access to Reddit to identify any individual or
> to associate any Reddit account with a particular individual or their
> personal information… [or] use Reddit data to target an individual
> Redditor for any purpose, including based on health, financial status or
> condition, political affiliation or beliefs, sex life or sexual
> orientation, racial or ethnic origin, religious or philosophical
> affiliation or beliefs, trade union membership…"*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| No individual identification | The `author` field is pseudonymised at ingestion time. The schema rejects any value that looks like a raw username: only `[deleted]`, `[removed]`, values prefixed with `user_`, `anon_`, or `pseudonym_`, or a 64-character SHA-256 hex digest are accepted. The external-data importer enforces this via the `is_seemingly_raw_username()` gate. | `scripts/import_external_data.py` (`is_seemingly_raw_username`), `utils/anonymize_data.py` (SHA-256 pseudonymisation), `docs/data_schema.md` (PII constraints section), `core/schemas.py` (`CollectedItem`) |
| No cross-platform identity linkage | The tool has no code path that joins Reddit data with any external identifier, email, IP, browser fingerprint, or cross-platform handle. | Audit via `grep -r "email\|fingerprint\|ip_address" .` (returns no such code) |
| No user-level profiling or segmentation | The analysis unit is the post, not the user. The PPM coding pipeline operates on post text and metadata; no module builds per-user histories, segments, or profiles. The dashboard aggregates by subreddit, theme, and PPM category, never by user. | `modules/llm_coder.py`, `modules/topic_modeling.py`, `modules/dashboard.py` |
| No targeting on protected characteristics | The research question concerns aggregate consumer discourse around nootropics as a product category. It does not attempt to infer, classify, or target individual Redditors by any of the protected categories listed in §2. Nootropics overlap with the "health" category in the addendum, but §2's prohibition is specifically on *targeting individuals* on the basis of health — not on studying health-adjacent consumer discourse at population scale. See "Research framing note" below. | `docs/research_scope.md` *(forthcoming)* |
| No surveillance use | The tool has no facial recognition, background-check, credit-scoring, or law-enforcement use path. Nothing collected is ever transmitted to any third party. | Whole-repo audit — no such dependencies in `requirements.txt` |

#### §2.c — Respect Redditors who choose to delete their content

> *"Within 3 days prior to publication submission, you must ensure that
> your published results do not include any Reddit data that was deleted
> prior to that date and that you delete any such data in your
> possession."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Automated deletion compliance scrubber | `scripts/scrub_deleted_data.py` authenticates via PRAW, iterates every collected item, calls `reddit.info(fullnames=...)` to detect deletion or moderator removal upstream, and scrubs matching local records. Supports `--dry-run` for auditing. | `scripts/scrub_deleted_data.py` |
| Pre-publication deletion check operationalised | The 3-day pre-publication deletion run is documented in the data handling workflow and will be invoked manually before every thesis draft submission and before every conference/journal submission. A dry-run log of a prior scrub run is committed as evidence. | `docs/data_handling.md` *(forthcoming)*, `docs/compliance_evidence/scrub_log.txt` |
| No offline copies beyond research need | The SQLite database used for research is the single source of truth; there is no separate exported data store. Streamlit session state is ephemeral. | `.gitignore` excludes `data/`, `exports/`, `*.csv`, `*.xlsx`, `*.zip` |

#### §2.d — No redistribution or commercialization

> *"Your access to Reddit data is solely in support of the research you
> identified to us and may not be used to redistribute or transfer the
> data to anyone else for any other purpose. You may not commercialize
> your use of the Reddit data in any form… You shall not make the data
> publicly available to anyone, including other researchers for other
> research projects, without Reddit's written permission."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| No Reddit data committed to repository | The public GitHub repository contains zero real Reddit content. The only "sample" data shipped is an entirely fabricated synthetic corpus written by the researcher for pipeline testing — it is generated by `scripts/generate_mock_ppm_data.py`, which contains the complete corpus as a hand-written Python list inside the file itself, and produces `samples/synthetic_nootropics_sample.csv` on demand. Every row in the generated CSV carries a `SYNTHETIC=TRUE` column flag, synthetic `author` values, and `.example` TLD permalinks that cannot resolve to any real resource. | `scripts/generate_mock_ppm_data.py` (source of truth), `samples/README.md` (provenance statement) |
| No data redistribution through any channel | No export, share, upload, publish, or transmit code path exists. The `modules/thesis_export.py` module exports analysis *results* (coded themes, aggregated statistics, codebook) — not raw Reddit data. | `modules/thesis_export.py` |
| No commercialization | Code is licensed PolyForm Noncommercial 1.0.0. No commercial use is permitted under the license, and no commercial use is planned or foreshadowed. The prior `CC-BY-4.0` data-license section that had appeared in an earlier revision of `LICENSE` was removed in commit `9e76931`. | `LICENSE`, commit history |
| No data sharing with other researchers | The repository documentation explicitly directs anyone wishing to reproduce this research to apply for their own Reddit4Researcher access rather than requesting data from the author. | `README.md`, `samples/README.md` |

### §3 — Publication

> *"In the exercise of the rights of academic freedom… you shall have the
> right to publish… the results of your research conducted under this
> Agreement, subject to these Terms… Open source your code. To the extent
> that your research involves the creation of algorithms, models, or
> code, as a matter of academic transparency, you must make your research
> and the underlying code publicly available without charge (e.g., under
> non-commercial license terms recognized by the Open Source Initiative)."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Right to publish exercised | The entire research codebase is public at [github.com/Hotpopize/NootropicRedditScrapePPM](https://github.com/Hotpopize/NootropicRedditScrapePPM). The thesis itself will be published through Modul University Vienna's thesis repository upon completion. | This repository |
| Non-commercial open licence | Code is licensed under the PolyForm Noncommercial License 1.0.0, a publicly-available source-available licence drafted specifically to satisfy the "non-commercial open source" use case that the Reddit addendum §3 describes. | `LICENSE` |
| Advance courtesy copy before publication | Prior to any conference or journal submission, a courtesy copy of the paper will be sent to the Reddit for Researchers team via the address on file at least one week in advance, as requested in §3. | Committed as operational policy in `docs/data_handling.md` *(forthcoming)* |
| Research reproducibility for other researchers | The analysis pipeline is decoupled from the collection layer via the `CollectedItem` schema contract documented in `docs/data_schema.md`, so any future researcher with their own Reddit4Researcher access (or with data from another lawful source) can reproduce the PPM coding methodology on their own data. The synthetic sample at `samples/synthetic_nootropics_sample.csv` allows end-to-end evaluation of the pipeline without any Reddit credentials at all. | `docs/data_schema.md`, `scripts/import_external_data.py`, `samples/` |

### §4 — Termination

> *"Upon the earlier of termination or completion of your research, you
> will permanently delete any Reddit data and derivatives thereof, except
> that upon completion of your research you may retain and securely store
> any data that is reasonably required and customarily retained in
> connection with such completed research."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| End-of-research deletion commitment | Upon thesis defence and final submission, all collected raw Reddit data will be permanently deleted from the local SQLite database via `scripts/scrub_deleted_data.py --purge-all` (or equivalent). Only the minimum derivative artefacts customarily retained for academic integrity — the PPM codebook, aggregated coding results, and the thesis itself — will be preserved, and none of those artefacts contain raw Reddit content. | `docs/data_handling.md` *(forthcoming)* |
| On-demand termination | The tool can be terminated and data purged at any time at Reddit's request. A single command purges the local research database. | `scripts/scrub_deleted_data.py`, `docs/data_handling.md` *(forthcoming)* |

### §6.4 — Attribution and Publicity

> *"You agree to appropriately attribute your source data received from
> Reddit and will display any attributions required by Reddit as described
> in the research platform documentation… You will not make any statement
> regarding your use of the Researcher Services, Content, or data which
> suggests partnership with, sponsorship by, or endorsement by Reddit,
> without Reddit's prior written approval."*

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Accurate attribution | The thesis will attribute Reddit as the data source and will cite the Reddit for Researchers programme and the executed addendum in the methodology chapter. | Thesis methodology chapter (in preparation) |
| No implied endorsement | No README copy, no repository description, no thesis-related communication, and no publication draft will claim partnership with, sponsorship by, or endorsement from Reddit. Language in this repository is carefully limited to the factual statement that research access is granted under a signed Reddit4Researcher Agreement. | `README.md`, `COMPLIANCE.md` (this document) |

---

## Research framing note — why this is not "health targeting"

The Addendum §2 prohibition on targeting individual Redditors based on
"health, financial status or condition…" is a prohibition on
**individual-level profiling**. It is not a prohibition on studying
health-adjacent consumer topics in aggregate. Published Reddit-based
academic research covers mental health communities, addiction recovery,
dietary interventions, and pharmaceutical discussion — none of which is
prohibited by §2, because none of it builds identifiable user profiles.

This project is framed explicitly at the population-discourse level. Its
compliance posture with respect to §2 rests on four concrete design
choices, each of which is enforced in code, not just policy:

1. The analysis unit is the post, not the user. No module in this
   repository constructs per-user histories, profiles, or segments.
2. Usernames are pseudonymised before any data reaches the analysis
   pipeline, enforced by `scripts/import_external_data.py` and
   documented in `docs/data_schema.md`.
3. The research question concerns *consumer product-switching discourse*
   (caffeine → prescription stimulants → natural nootropics) as a
   management/marketing phenomenon — it is located in the consumer
   behaviour literature, not the clinical literature. This is a thesis
   at a management school (Modul University Vienna, MSc Management / MBA).
4. No diagnostic, clinical, or treatment claims are made or intended
   about any individual. No real username, no matter how anonymised, is
   ever reproduced in the thesis.

If Reddit's review team has a different interpretation of §2 that would
still put this research out of scope despite the above, the author is
open to narrowing the scope further (e.g. excluding specific subreddits,
additional keyword filtering, supplementary anonymisation) and welcomes
specific guidance via the support ticket referenced above.

---

## Related documents

| Document | Purpose |
|---|---|
| `README.md` | Project overview, quickstart with synthetic sample, links to compliance documentation |
| `LICENSE` | PolyForm Noncommercial 1.0.0 — the non-commercial licence referenced in §3 |
| `docs/data_schema.md` | The `CollectedItem` contract that downstream analysis operates on; includes the PII constraints enforced at ingestion |
| `docs/research_scope.md` | Concrete scope: target subreddits, keyword list, date range, endpoints used, rate-limit ceiling, expected post volume |
| `docs/data_handling.md` | Lifecycle policy: storage location, retention period, pre-publication deletion check workflow, end-of-research deletion commitment |
| `docs/compliance_evidence/scrub_log.txt` | Committed log of a prior `scrub_deleted_data.py --dry-run` execution |
| `samples/README.md` | Provenance statement for the synthetic sample dataset |
| `scripts/scrub_deleted_data.py` | Automated §2.c deletion compliance scrubber |
| `scripts/import_external_data.py` | External-data ingestion with enforced PII constraints |
| `utils/anonymize_data.py` | SHA-256 username pseudonymisation for any remaining raw usernames |
| `services/reddit_service.py` | Authenticated PRAW client — the single access path to Reddit data |
| `app.py` | Startup credential gate (lines 66–100) |

---

## Change log

| 2026-04-13 | added r/NooTopics as sixth target subreddit; corrected r/Biohackers metadata | `3c149a4` |
| 2026-04-09 | Initial compliance map written | *(pending merge)* |
| 2026-04-09 | Tier 3 BYO-data interface and synthetic sample added | `5f78cb9` |
| 2026-04-09 | Hard startup credential gate added | `d5e6faa` |
| 2026-04-09 | `LICENSE` rewritten to PolyForm Noncommercial 1.0.0; CC-BY data-licence section removed | `9e76931` |
| 2026-04-09 | Unauthenticated JSON endpoint path deleted | `0182153` |
| 2026-04-01 | Research Data Addendum executed by both parties | — |

---

*This document is maintained by the researcher. For compliance
questions, contact 1821019@modul.ac.at.*
