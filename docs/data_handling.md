# Data Handling & Lifecycle Policy

**Project:** NootropicRedditScrapePPM  
**Policy Status:** Active (Compliant with Reddit Research Data Addendum)

---

## 1. Data Lifecycle Overview
The data lifecycle is strictly linear and local to the researcher's environment:
1. **Authenticated Ingestion**: Data enters via PRAW (`services/reddit_service.py`).
2. **Immediate Pseudonymisation**: Usernames are hashed with SHA-256 before being written to disk.
3. **Local Analysis**: LLM coding and statistical aggregation performed on the local SQLite DB.
4. **Scrubbing/Purging**: Automated deletion checks and final research-end purge.

## 2. PII Protection (Pseudonymisation)
Identification of individual Redditors is prohibited by §2.b of the Addendum. To operationalize this:
- **At Ingestion:** The `CollectedItem` schema in `core/schemas.py` enforces a constraint on the `author` field.
- **Hashing Algorithm:** `hashlib.sha256(username.encode()).hexdigest()`.
- **Relational Integrity:** Hashing is deterministic within the researcher's environment, allowing for tracking of the same (anonymous) user across threads without knowing their identity.
- **Scrubbing:** Raw usernames never reach the SQLite database or any application log.

## 3. Storage & Security
- **Local SQLite DB:** All research data is stored in `research_data.db`.
- **Zero Redistribution:** The database and all data exports (`data/`, `exports/`, `logs/`) are explicitly excluded from version control via `.gitignore`.
- **No Cloud Dependency:** Application and data reside solely on the researcher's local machine.

## 4. Deletion Compliance (§2.c)
The project implements a rigorous content deletion workflow:
- **The Tool:** `scripts/scrub_deleted_data.py` (The Scrubber).
- **Function:** Cross-references every local record against the live Reddit API (`reddit.info()`).
- **Operational Cadence:**
    - **Draft Submission:** The Scrubber is run within 72 hours prior to any thesis draft submission.
    - **Final Defense:** The Scrubber is run before the final thesis deposition.
    - **End of Research:** Upon completion of the thesis and defense, all raw Reddit data is purged using the `--purge-all` flag.

## 5. Audit logs
The repository maintains a committed log of prior compliance scrubs in `docs/compliance_evidence/scrub_log.txt`. This provides an audit trail for reviewers without exposing any user content.

---
*For the full compliance mapping, see [COMPLIANCE.md](../COMPLIANCE.md).*
