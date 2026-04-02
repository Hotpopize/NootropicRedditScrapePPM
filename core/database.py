"""
core/database.py
================
Database models and initialisation for NootropicRedditScrapePPM.

Tables
------
Core (thesis-critical):
  collected_data      — raw Reddit posts/comments
  coded_data          — LLM PPM coding results
  codebook            — PPM code definitions
  scrape_runs         — job tracking per collection run

Compliance (audit trail):
  audit_log           — discrete action log
  replicability_log   — full parameter + statistics record per collection

Inactive (future / Zotero integration):
  zotero_references
  zotero_collection_links

Migration notes
---------------
- data_source column added to collected_data (safe ALTER TABLE guard in init_db)
- init_db() MUST be defined after all model classes so Base.metadata is populated
"""

import os
import logging
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Text,
    DateTime, JSON, Float, text, inspect
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    os.makedirs('data', exist_ok=True)
    DATABASE_URL = "sqlite:///data/research_data.db"
    engine = create_engine(
        DATABASE_URL,
        connect_args={'check_same_thread': False},
        pool_pre_ping=True
    )
else:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )

SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

Base = declarative_base()

# ---------------------------------------------------------------------------
# Core models — thesis-critical
# ---------------------------------------------------------------------------

class CollectedData(Base):
    """Raw Reddit posts and comments collected via PRAW or JSON endpoint."""
    __tablename__ = 'collected_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    reddit_id = Column(String, unique=True, index=True, nullable=False)
    type = Column(String)  # 'submission' | 'comment'
    subreddit = Column(String, index=True)
    title = Column(Text)
    text = Column(Text)
    author = Column(String)
    score = Column(Integer)
    created_utc = Column(Float)
    num_comments = Column(Integer, nullable=True)
    url = Column(Text, nullable=True)
    permalink = Column(Text)
    post_id = Column(String, nullable=True, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(String, index=True)
    data_source = Column(String, nullable=True, default='praw')
    # Use JSON for flexible metadata storage
    extra_metadata = Column(JSON, nullable=True)


class CodedData(Base):
    """LLM-assisted PPM coding results per Reddit item."""
    __tablename__ = 'coded_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    reddit_id = Column(String, index=True)
    ppm_category = Column(String, index=True)
    ppm_subcodes = Column(JSON)
    themes = Column(JSON)
    evidence_quotes = Column(JSON)
    confidence = Column(String)
    coded_at = Column(DateTime, default=datetime.utcnow)
    coded_by = Column(String)  # 'llm:llama3.1' | 'llm:gemma3:12b' | 'human'
    coding_approach = Column(String)
    session_id = Column(String, index=True)
    rationale = Column(Text, nullable=True)
    raw_prompt = Column(Text, nullable=True)
    raw_response = Column(Text, nullable=True)
    extra_metadata = Column(JSON, nullable=True)


class Codebook(Base):
    """PPM codebook — PUSH/PULL/MOOR-F/MOOR-I/EMER code definitions."""
    __tablename__ = 'codebook'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String, index=True)
    name = Column(String, index=True)
    definition = Column(Text)
    examples = Column(Text, nullable=True)
    frequency = Column(Integer, default=0)
    added_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(String, index=True)
    extra_metadata = Column(JSON, nullable=True)


class ScrapeRun(Base):
    """One record per background collection job — tracks status and item count."""
    __tablename__ = 'scrape_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, unique=True, index=True)
    status = Column(String, index=True)  # RUNNING|COMPLETED|FAILED|CANCELLED
    config_hash = Column(String, index=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    items_collected = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    session_id = Column(String, index=True, nullable=True)
    parameters = Column(JSON)
    extra_metadata = Column(JSON, nullable=True)

# ---------------------------------------------------------------------------
# Compliance models — audit trail
# ---------------------------------------------------------------------------

class AuditLog(Base):
    """
    Discrete action log — one row per user/system action.
    Used for methodological transparency per netnographic ethics standards.
    """
    __tablename__ = 'audit_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    action = Column(String, index=True)
    session_id = Column(String, index=True)
    user_info = Column(String, nullable=True)
    details = Column(JSON)
    extra_metadata = Column(JSON, nullable=True)


class ReplicabilityLog(Base):
    """
    Full parameter + statistics record per collection run.
    Authoritative audit trail for Chapter 3 methodology documentation.
    One row per collection_hash (unique per parameter set).
    """
    __tablename__ = 'replicability_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    collection_hash = Column(String, unique=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    session_id = Column(String, index=True)
    parameters = Column(JSON)  # includes data_source field
    statistics = Column(JSON)
    rate_limit_events = Column(JSON, nullable=True)
    validation_results = Column(JSON, nullable=True)
    notes = Column(Text, nullable=True)

# ---------------------------------------------------------------------------
# Inactive models — Zotero integration (future)
# ---------------------------------------------------------------------------

class ZoteroReference(Base):
    """Synced Zotero citation records. Not used in core thesis pipeline."""
    __tablename__ = 'zotero_references'

    id = Column(Integer, primary_key=True, autoincrement=True)
    zotero_key = Column(String, unique=True, index=True)
    item_type = Column(String)
    title = Column(Text)
    authors = Column(JSON)
    year = Column(String, nullable=True)
    abstract = Column(Text, nullable=True)
    doi = Column(String, nullable=True, index=True)
    url = Column(Text, nullable=True)
    tags = Column(JSON)
    collections = Column(JSON)
    keywords = Column(JSON)
    citation_apa = Column(Text, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(String, index=True)
    extra_metadata = Column(JSON, nullable=True)


class ZoteroCollectionLink(Base):
    """Links between collection hashes and Zotero keys. Not used in core pipeline."""
    __tablename__ = 'zotero_collection_links'

    id               = Column(Integer, primary_key=True, autoincrement=True)
    collection_hash  = Column(String, index=True)
    zotero_key       = Column(String, index=True)
    link_type        = Column(String)
    relevance_score  = Column(Float, nullable=True)
    matched_keywords = Column(JSON, nullable=True)
    linked_at        = Column(DateTime, default=datetime.utcnow)
    session_id       = Column(String, index=True)
    notes            = Column(Text, nullable=True)


class EmergentCandidate(Base):
    """
    Temporary storage for candidate subcodes proposed by LLM during coding.
    Persists the 'emergent queue' across browser refreshes.
    """
    __tablename__ = 'emergent_candidates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String)  # Push/Pull/Mooring
    name = Column(String)
    definition = Column(Text)
    evidence = Column(Text)
    reddit_id = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    session_id = Column(String, index=True)
    status = Column(String, default='pending', index=True)  # pending, approved, rejected


# ---------------------------------------------------------------------------
# init_db — MUST remain below all model definitions
# Base.metadata is empty until the model classes above are parsed by Python.
# Calling init_db() before models are defined creates zero tables silently.
# ---------------------------------------------------------------------------

def init_db():
    """
    Create all tables and apply safe runtime migrations.
    Called once at app startup from app.py.
    """
    # Register all models on Base and create missing tables
    Base.metadata.create_all(bind=engine)

    if "sqlite" not in str(engine.url):
        return

    with engine.connect() as conn:
        # WAL mode — better concurrency for Streamlit's threaded model
        conn.execute(text("PRAGMA journal_mode=WAL;"))
        conn.commit()

    # -----------------------------------------------------------------------
    # Safe migration: data_source column on collected_data
    # Only runs on existing DBs where the column predates the model change.
    # Fresh installs already have the column from create_all above.
    # -----------------------------------------------------------------------
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    if 'collected_data' in existing_tables:
        cols = [c["name"] for c in inspector.get_columns("collected_data")]
        if "data_source" not in cols:
            logger.info("Migrating: adding data_source column to collected_data")
            with engine.connect() as conn:
                conn.execute(text(
                    "ALTER TABLE collected_data "
                    "ADD COLUMN data_source VARCHAR DEFAULT 'praw'"
                ))
                conn.commit()
            logger.info("Migration complete: data_source column added")

    if 'coded_data' in existing_tables:
        cols = [c["name"] for c in inspector.get_columns("coded_data")]
        with engine.connect() as conn:
            migration_needed = False
            if "raw_prompt" not in cols:
                conn.execute(text("ALTER TABLE coded_data ADD COLUMN raw_prompt TEXT"))
                migration_needed = True
            if "raw_response" not in cols:
                conn.execute(text("ALTER TABLE coded_data ADD COLUMN raw_response TEXT"))
                migration_needed = True
            if migration_needed:
                conn.commit()
                logger.info("Migration complete: raw_prompt/raw_response columns added")

# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def get_db():
    """FastAPI-style dependency yielding a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """Direct session access for non-dependency-injection contexts."""
    return SessionLocal()
