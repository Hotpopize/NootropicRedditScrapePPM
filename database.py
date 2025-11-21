import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///research_data.db"
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)

SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()

class CollectedData(Base):
    __tablename__ = 'collected_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    reddit_id = Column(String, unique=True, index=True)
    type = Column(String)
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
    extra_metadata = Column(JSON, nullable=True)

class CodedData(Base):
    __tablename__ = 'coded_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    reddit_id = Column(String, index=True)
    ppm_category = Column(String, index=True)
    ppm_subcodes = Column(JSON)
    themes = Column(JSON)
    evidence_quotes = Column(JSON)
    confidence = Column(String)
    coded_at = Column(DateTime, default=datetime.utcnow)
    coded_by = Column(String)
    coding_approach = Column(String)
    session_id = Column(String, index=True)
    rationale = Column(Text, nullable=True)
    extra_metadata = Column(JSON, nullable=True)

class Codebook(Base):
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

class AuditLog(Base):
    __tablename__ = 'audit_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    action = Column(String, index=True)
    session_id = Column(String, index=True)
    user_info = Column(String, nullable=True)
    details = Column(JSON)
    extra_metadata = Column(JSON, nullable=True)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_session():
    return SessionLocal()
