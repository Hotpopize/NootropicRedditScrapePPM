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

## Recent Changes (2025-11-21)
- Added PostgreSQL database with complete persistence layer
- Implemented database models for all research data types
- Fixed Excel export using BytesIO buffers
- Added environment variable validation for LLM integration
- Implemented data loading on startup for session continuity
- Added fallback to SQLite when DATABASE_URL not configured
- All modules now save to database automatically
- Audit logging moved from flat files to database

## Development Notes
- Database initialization happens automatically on app startup
- Session state loads all historical data from database
- Supports both PostgreSQL (via DATABASE_URL) and SQLite fallback
- OpenAI integration uses Replit AI Integrations (charges billed to credits)
- Reddit credentials stored in session state (not persisted)

## Future Enhancements Planned
- BERTopic integration for topic modeling validation
- Inter-coder reliability metrics
- GPU-accelerated batch processing with RAPIDS cuDF
- Export templates for thesis appendices
- Citation manager integration