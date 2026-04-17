"""
HireLens – Database Models & Connection
"""
 
import os
from datetime import datetime
from typing import Optional
 
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, create_engine, text
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker
 
load_dotenv()
 
 
class Base(DeclarativeBase):
    pass
 
 
class JobPosting(Base):
    __tablename__ = "job_postings"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    external_id     = Column(String(255), unique=True, nullable=False, index=True)
    source          = Column(String(50), nullable=False)
    title           = Column(String(500), nullable=False)
    company         = Column(String(255))
    location        = Column(String(255))
    is_remote       = Column(Boolean, default=False)
    salary_min      = Column(Float)
    salary_max      = Column(Float)
    salary_currency = Column(String(10), default="USD")
    description_raw = Column(Text)
    url             = Column(Text)
    posted_date     = Column(DateTime)
    scraped_at      = Column(DateTime, default=datetime.utcnow)
    is_processed    = Column(Boolean, default=False)
    processed = relationship("ProcessedJob", back_populates="posting", uselist=False)
 
 
class ProcessedJob(Base):
    __tablename__ = "processed_jobs"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    posting_id      = Column(Integer, ForeignKey("job_postings.id"), unique=True)
    role_category   = Column(String(100), index=True)
    seniority       = Column(String(50))
    skills          = Column(ARRAY(Text))
    tools           = Column(ARRAY(Text))
    description_clean = Column(Text)
    processed_at    = Column(DateTime, default=datetime.utcnow)
    posting = relationship("JobPosting", back_populates="processed")
 
 
class SkillTrend(Base):
    __tablename__ = "skill_trends"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    skill       = Column(String(255), nullable=False, index=True)
    category    = Column(String(100), index=True)
    count       = Column(Integer, default=0)
    snapshot_date = Column(DateTime, default=datetime.utcnow)
 
 
class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    started_at      = Column(DateTime, default=datetime.utcnow)
    finished_at     = Column(DateTime)
    jobs_scraped    = Column(Integer, default=0)
    jobs_processed  = Column(Integer, default=0)
    errors          = Column(Integer, default=0)
    status          = Column(String(50), default="running")
    meta            = Column(JSONB)
 
 
def get_engine(url: Optional[str] = None):
    db_url = url or os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hirelens")
    return create_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)
 
 
def get_session_factory(engine=None):
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)
 
 
def init_db(engine=None):
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database tables initialised.")
    return engine
 
 
def get_db_session(engine=None) -> Session:
    return get_session_factory(engine)()
 
 
def check_connection(engine=None) -> bool:
    try:
        eng = engine or get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        return False
 
