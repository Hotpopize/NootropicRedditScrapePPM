# Academic Research Tool - Natural Cognitive Supplement Market Segmentation

## Overview
This is a Streamlit-based academic research application designed for qualitative analysis of Reddit data related to natural cognitive supplements (nootropics). The tool supports systematic data collection, LLM-assisted thematic coding using the Push-Pull-Mooring (PPM) framework, and rigorous methodological documentation aligned with Creswell & Creswell (2023) standards.

## Purpose
Supporting PhD/Masters thesis research on consumer behavior in the natural cognitive supplement market through:
- Reddit data collection via PRAW API
- LLM-assisted thematic coding (OpenAI via Replit AI Integrations)
- PPM framework application with emergent theme detection
- Export compatibility with NVivo, MAXQDA, and statistical tools
- Complete audit trail for methodological transparency

## Project Architecture

### Core Components
1. **app.py**: Main Streamlit application with navigation and session management
2. **database.py**: SQLAlchemy models and database configuration (PostgreSQL/SQLite)
3. **utils/db_helpers.py**: Database persistence layer for all research data
4. **modules/**: Modular UI components for different research functions

### Database Schema
- **collected_data**: Reddit posts and comments with full metadata
- **coded_data**: LLM-assisted coding results with PPM categorization
- **codebook**: Theoretical and emergent code definitions
- **audit_log**: Complete methodological audit trail
- **replicability_log**: Collection parameters, statistics, and validation metrics for reproducibility
- **zotero_references**: Synced Zotero library citations with extracted keywords
- **zotero_collection_links**: Citation-to-collection links for audit trail documentation

### Key Features Implemented
- ✅ Reddit data collection with PRAW integration
- ✅ LLM-assisted thematic coding (gpt-5, gpt-4.1, etc.)
- ✅ PPM framework coding (Push/Pull/Mooring factors)
- ✅ Codebook management with frequency tracking
- ✅ PostgreSQL/SQLite persistence for data continuity
- ✅ CSV/JSON/Excel export for external analysis
- ✅ Session logging and audit trails
- ✅ Interactive dashboard with statistics

### Technical Stack
- **Frontend**: Streamlit 1.51+
- **Database**: PostgreSQL (production) / SQLite (development)
- **LLM Integration**: OpenAI via Replit AI Integrations (no API key required)
- **Reddit API**: PRAW 7.8+
- **Data Processing**: Pandas, SQLAlchemy
- **Error Handling**: Tenacity for retry logic

## User Preferences
- Academic rigor and replicability are paramount
- Data must persist across sessions for long-term research projects
- Export formats must be compatible with NVivo and MAXQDA
- All methodological decisions must be logged
- LLM coding requires human validation and oversight

## Recent Changes (2025-11-30)
- ✅ Added Zotero citation manager integration with pyzotero library
- ✅ New database models: ZoteroReference, ZoteroCollectionLink for citation persistence
- ✅ Zotero API sync with keyword extraction from abstracts and tags
- ✅ Literature-guided data collection: Zotero keywords available in Reddit scraper
- ✅ Citation-to-collection linking for audit trail documentation
- ✅ New Thesis Export Appendix G: Literature-Data Linkages
- ✅ APA citation generation from Zotero metadata
- ✅ Keyword selection UI for theoretically-grounded search queries

## Previous Changes (2025-11-27)
- ✅ Added comprehensive NSFW content handling with include/exclude toggle
- ✅ Implemented edge case detection: removed/deleted content, media-only posts, non-English text, truncated content
- ✅ Added ReplicabilityLog database model for collection parameter persistence and verification
- ✅ Implemented collection hash generation for replicability verification
- ✅ Rate limit event tracking and logging
- ✅ Complete validation metrics: NSFW collected/skipped, removed collected/skipped, truncated items, media-only items
- ✅ NSFW subreddit tracking persisted to database for fresh session access
- ✅ Thesis Export Appendix F now database-driven with full edge case documentation
- ✅ Data source indicator (DATABASE/SESSION) in Appendix F for transparency
- ✅ Safe denominators prevent division-by-zero in percentage calculations

## Previous Changes (2025-11-21)
- ✅ Added PostgreSQL database with complete persistence layer
- ✅ Implemented database models for all research data types
- ✅ Fixed Excel export using BytesIO buffers
- ✅ Added environment variable validation for LLM integration
- ✅ Implemented data loading on startup for session continuity
- ✅ Added fallback to SQLite when DATABASE_URL not configured
- ✅ All modules now save to database automatically
- ✅ Audit logging moved from flat files to database
- ✅ Added Topic Modeling module (TF-IDF, LDA, NMF) for automated theme discovery
- ✅ Added Inter-Coder Reliability module with Cohen's Kappa and Krippendorff's Alpha
- ✅ Added Thesis Export Templates for all academic appendices (A-E) and methodology chapter
- ✅ Fixed codebook deletion persistence to properly propagate to database
- ✅ Corrected Krippendorff's Alpha implementation to follow canonical formula

## Development Notes
- Database initialization happens automatically on app startup
- Session state loads all historical data from database (session-independent)
- Supports both PostgreSQL (via DATABASE_URL) and SQLite fallback (automatic)
- OpenAI integration uses Replit AI Integrations (charges billed to credits)
- Reddit credentials stored in session state (not persisted)
- All save operations use upsert pattern to prevent duplicates
- Codebook deletion properly removes entries from database
- Inter-coder reliability uses canonical Krippendorff Alpha: α = 1 - (Do/De)
- ReplicabilityLog stores collection_hash, parameters, statistics, validation_results for each run
- NSFW metadata captured at both post and subreddit level (over_18 flags)
- Edge case handling documented in Appendix F with database-sourced metrics

## Completed Features (Production-Ready)
- ✅ Reddit data collection with PRAW integration and retry logic
- ✅ NSFW content handling with include/exclude toggle and metadata capture
- ✅ Edge case detection (removed/deleted, media-only, non-English, truncated)
- ✅ Replicability logging with collection hash for reproducibility verification
- ✅ LLM-assisted thematic coding with OpenAI GPT-5
- ✅ PPM framework coding (Push/Pull/Mooring factors)
- ✅ Codebook management with frequency tracking and persistence
- ✅ PostgreSQL/SQLite persistence for data continuity
- ✅ CSV/JSON/Excel export for NVivo and MAXQDA
- ✅ Topic modeling (TF-IDF, LDA, NMF) for theme validation
- ✅ Inter-coder reliability (Cohen's Kappa, Krippendorff's Alpha)
- ✅ Thesis export templates (Appendices A-G, Methodology Chapter)
- ✅ Session logging and audit trails
- ✅ Interactive dashboard with statistics
- ✅ Zotero citation manager integration
- ✅ Literature-guided data collection with keyword extraction
- ✅ End-to-end testing passed

## Future Enhancements (Optional)
- GPU-accelerated batch processing with RAPIDS cuDF
- BERTopic integration as alternative to TF-IDF/LDA
- Advanced visualization tools
- Multi-user collaboration features