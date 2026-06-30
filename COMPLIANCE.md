# Compliance Map — Reddit Research Data Addendum

This document maps every substantive clause of the Reddit Research Data Addendum (executed 2026-04-01) to the specific code, documentation, policy, or operational control in this repository that operationalises it. It exists so that any reviewer — whether from Reddit, the Modul University Vienna examining committee, or a future researcher considering reuse of this tool — can audit compliance without having to read the full source tree.

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
| **Agreement** | Reddit Research Data Addendum (08192024), executed 2026-04-01 |
| **Reddit signatory** | Andrea Middleton, Director, Community Empowerment |
| **Researcher signatory** | Vladislav Dolgov, Modul University Vienna |
| **DocuSign envelope** | `<ADDENDUM_ID>` |
| **Researcher support ticket** | `[17VP91-EZDNE]` |

---

## Executive Summary

This tool processes raw Reddit data imported via CLI scripts from BigQuery (BQAH), stores it locally for the duration of an MSc/MBA thesis, and applies a local Large-Language-Model-assisted qualitative coding pipeline using the Push-Pull-Mooring (PPM) framework. The research question is aggregate and discursive: *how do consumers talk about switching between caffeine, prescription stimulants, and natural nootropics?* It is not clinical, not commercial, not individual-level, and not profile-building. The analysis unit is the post, not the user.

Three structural guarantees underpin the compliance posture below:

1. **No live API callers or unauthenticated access.** The instrument has no scraping functionality and contains no credentials or API connections. Data is imported offline via BQAH exports.
2. **No real Reddit data is redistributed.** The repository contains no committed Reddit content of any kind. The only sample data shipped in the repository is an entirely fabricated synthetic corpus written by the researcher for testing, stored under `samples/` with synthetic markers.
3. **PII never enters the analysis pipeline.** Ingested author fields must be pseudonymized (e.g. SHA-256 hashed or anonymized) at the point of extraction/ingestion, conforming to the PII author constraint.

---

## Clause-by-clause compliance map

### §1 — Registration and Access

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Access only via authorised credentials | No collection pathways or API keys exist in this application. Ingestion is performed via CLI using BigQuery pre-exported datasets. | `scripts/bqah_import.py` |
| Local data stored securely | All collected data is held in a local SQLite database outside of version control. The database is excluded from version control via `.gitignore`. No committed artifact in this repository contains any Reddit user content. | `.gitignore` (lines: `*.db`, `*.db-shm`, `*.db-wal`) |

### 2. Research question and theoretical framework

**Major Research Question (MRQ):**
> *How do push, pull, and mooring factors exhibit across online nootropic
> communities, and what distinct community-level consumer profiles emerge
> from PPM distribution?*

**Sub-questions:**
- **SQ1:** What forms of dissatisfaction with conventional cognitive enhancers
  surface in community discourse, and how do PUSH factors vary across subreddits?
- **SQ2:** What attributes of natural nootropics attract community endorsement,
  and how do PULL factors differ between health-maintenance-oriented and
  cognitive-optimisation-oriented communities?
- **SQ3:** What contextual conditions facilitate or impede switching, and how
  do MOORING factors operate as both enablers (MOOR-F) and barriers (MOOR-I)?

**End goal:** community-level PPM analysis. Posts are the coding unit; the subreddit is the unit of comparative synthesis. Each community yields a PPM distribution profile, and profiles aggregate into consumer archetypes. The pipeline is therefore stratified by subreddit, not pooled.

**Framework:** Push-Pull-Mooring (Bansal et al., 2005; Moon, 1995; meta-analysed in Marx, 2025). Coding scheme:
- **PUSH** — dissatisfaction with incumbents (caffeine jitters/crash/tolerance; Rx stimulant side effects, comedowns, gatekeeping; energy-drink dependency).
- **PULL** — attraction to natural alternatives (lion's mane, rhodiola, bacopa, ashwagandha, omega-3, L-theanine, matcha, ginkgo, creatine-for-cognition).
- **MOOR-F (facilitators)** — contextual conditions that *enable* switching (lifestyle integration, systems thinking, supportive community norms, accessible product knowledge). Concentrated in r/Supplements and r/Biohackers.
- **MOOR-I (inhibitors)** — switching barriers (cost, EU regulatory uncertainty, dosing confusion, placebo doubt, physician dismissal, ethical discomfort, ritual attachment, prior disappointment). Concentrated in r/StackAdvice.
- **MIXED** — posts spanning multiple categories.

The MOOR-F / MOOR-I split is non-optional: SQ3 explicitly requires both poles.

### 3. Target scope

**Subreddits (six), with PPM anchoring per Table 4:**

| Subreddit       | Primary PPM dimension          | RQ anchor   |
|-----------------|--------------------------------|-------------|
| r/Nootropics    | Mixed PUSH/PULL (evidence)     | P1, P2, MRQ |
| r/Supplements   | PULL-dominant + MOOR-F         | P2, P4      |
| r/Decaf         | PUSH-dominant                  | P1, P3      |
| r/Biohackers    | PULL + MOOR-F (systems)        | P2, P4      |
| r/StackAdvice   | MOOR-I concentrated            | P3, P4      |
| r/NooTopics     | Mixed PUSH/PULL (pharmacology) | MRQ         |

---

### §6.4 — Attribution and Publicity

| Requirement | Compliance posture | Where it lives |
|---|---|---|
| Accurate attribution | The thesis attributes Reddit as the data source and cites the Reddit for Researchers programme and the executed addendum in the methodology chapter. | Thesis methodology chapter (in preparation) |
| No implied endorsement | No README copy, no repository description, no thesis-related communication, and no publication draft will claim partnership with, sponsorship by, or endorsement from Reddit. Language in this repository is carefully limited to the factual statement that research access is granted under a signed Reddit4Researcher Agreement. | `README.md`, `COMPLIANCE.md` (this document) |

---

## Research framing note — why this is not "health targeting"

The Addendum §2 prohibition on targeting individual Redditors based on "health, financial status or condition…" is a prohibition on **individual-level profiling**. It is not a prohibition on studying health-adjacent consumer topics in aggregate.

This project is framed explicitly at the population-discourse level. Its compliance posture with respect to §2 rests on four concrete design choices:

1. The analysis unit is the post, not the user. No module in this repository constructs per-user histories, profiles, or segments.
2. Usernames are pseudonymised before any data reaches the analysis pipeline, enforced at BQAH ingestion time.
3. The research question concerns *consumer product-switching discourse* (caffeine -> prescription stimulants -> natural nootropics) as a management/marketing phenomenon — it is located in the consumer behaviour literature, not the clinical literature. This is a thesis at a management school (Modul University Vienna, MSc Management / MBA).
4. No diagnostic, clinical, or treatment claims are made or intended about any individual. No real username, no matter how anonymised, is ever reproduced in the thesis.

---

## Related documents

| Document | Purpose |
|---|---|
| `README.md` | Project overview, quickstart with synthetic sample, links to compliance documentation |
| `LICENSE` | MIT License — the open-source licence governing the code |
| `docs/Quote_Admission_Rules.md` | Qualitative quote selection logic |
| `docs/bqah_extraction_reference.md` | Technical BigQuery search parameters |
| `scripts/bqah_import.py` | CLI import tool for pre-exported BQAH datasets |
| `scripts/bqah_coder.py` | CLI qualitative qualitative coding tool utilizing local Ollama instances |
| `app.py` | Streamlit Dashboard Router |

---

## Change log

| Date | Description | Ref |
|---|---|---|
| 2026-06-30 | v1.2 — removed PRAW scraper, credentials gate, and live API references; updated to BQAH-import architecture | — |
| 2026-04-13 | v1.1 — added MOOR-F/MOOR-I split, RQ anchoring, stratified quota, tiered LIMIT defaults, §5.H quality filter; confirmed hashed_user_id and created_utc | (pending) |
| 2026-04-13 | added r/NooTopics as sixth target subreddit; corrected r/Biohackers metadata | `3c149a4` |
| 2026-04-09 | Initial compliance map written | — |

---

*This document is maintained by the researcher. For compliance questions, contact 1821019@modul.ac.at.*
