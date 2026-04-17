"""
HireLens – Dashboard Queries
All data queries returning pandas DataFrames.
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import func, text

from models import (
    JobPosting, ProcessedJob, SkillTrend, PipelineRun,
    get_db_session, get_engine,
)


def _session():
    return get_db_session(get_engine())


def get_kpis() -> dict:
    session = _session()
    try:
        total_jobs       = session.query(func.count(JobPosting.id)).scalar() or 0
        processed_jobs   = session.query(func.count(ProcessedJob.id)).scalar() or 0
        unique_companies = session.query(func.count(func.distinct(JobPosting.company))).scalar() or 0
        remote_jobs      = session.query(func.count(JobPosting.id)).filter(JobPosting.is_remote == True).scalar() or 0
        unique_skills    = session.query(func.count(func.distinct(SkillTrend.skill))).filter(SkillTrend.category == "All").scalar() or 0
        return {
            "total_jobs": total_jobs, "processed_jobs": processed_jobs,
            "unique_companies": unique_companies, "remote_jobs": remote_jobs,
            "remote_pct": round(remote_jobs / total_jobs * 100, 1) if total_jobs else 0,
            "unique_skills": unique_skills,
        }
    finally:
        session.close()


def get_role_distribution() -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(ProcessedJob.role_category, func.count(ProcessedJob.id).label("count"))
                .group_by(ProcessedJob.role_category)
                .order_by(func.count(ProcessedJob.id).desc()).all())
        return pd.DataFrame(rows, columns=["role_category", "count"])
    finally:
        session.close()


def get_seniority_distribution(role_category: Optional[str] = None) -> pd.DataFrame:
    session = _session()
    try:
        q = session.query(ProcessedJob.seniority, func.count(ProcessedJob.id).label("count"))
        if role_category and role_category != "All":
            q = q.filter(ProcessedJob.role_category == role_category)
        rows = q.group_by(ProcessedJob.seniority).order_by(func.count(ProcessedJob.id).desc()).all()
        return pd.DataFrame(rows, columns=["seniority", "count"])
    finally:
        session.close()


def get_top_skills(category: str = "All", top_n: int = 20) -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(SkillTrend.skill, SkillTrend.count)
                .filter(SkillTrend.category == category)
                .order_by(SkillTrend.count.desc()).limit(top_n).all())
        return pd.DataFrame(rows, columns=["skill", "count"])
    finally:
        session.close()


def get_skill_by_role() -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(SkillTrend.skill, SkillTrend.category, SkillTrend.count)
                .filter(SkillTrend.category != "All")
                .order_by(SkillTrend.count.desc()).all())
        return pd.DataFrame(rows, columns=["skill", "role_category", "count"])
    finally:
        session.close()


def get_top_hiring_companies(top_n: int = 15) -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(JobPosting.company, func.count(JobPosting.id).label("count"))
                .group_by(JobPosting.company)
                .order_by(func.count(JobPosting.id).desc()).limit(top_n).all())
        return pd.DataFrame(rows, columns=["company", "count"])
    finally:
        session.close()


def get_salary_by_role() -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(
                    ProcessedJob.role_category,
                    func.avg(JobPosting.salary_min).label("avg_min"),
                    func.avg(JobPosting.salary_max).label("avg_max"),
                    func.count(JobPosting.id).label("postings_with_salary"),
                )
                .join(JobPosting, JobPosting.id == ProcessedJob.posting_id)
                .filter(JobPosting.salary_min.isnot(None))
                .group_by(ProcessedJob.role_category)
                .order_by(func.avg(JobPosting.salary_max).desc()).all())
        df = pd.DataFrame(rows, columns=["role_category", "avg_min", "avg_max", "postings_with_salary"])
        df["avg_min"] = df["avg_min"].round(0)
        df["avg_max"] = df["avg_max"].round(0)
        return df
    finally:
        session.close()


def get_location_distribution(top_n: int = 15) -> pd.DataFrame:
    session = _session()
    try:
        rows = (session.query(JobPosting.location, func.count(JobPosting.id).label("count"))
                .filter(JobPosting.location.isnot(None), JobPosting.location != "")
                .group_by(JobPosting.location)
                .order_by(func.count(JobPosting.id).desc()).limit(top_n).all())
        return pd.DataFrame(rows, columns=["location", "count"])
    finally:
        session.close()


def get_postings_over_time(days: int = 30) -> pd.DataFrame:
    session = _session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        rows = (session.query(
                    func.date_trunc("day", JobPosting.scraped_at).label("date"),
                    func.count(JobPosting.id).label("count"),
                )
                .filter(JobPosting.scraped_at >= cutoff)
                .group_by(text("date")).order_by(text("date")).all())
        return pd.DataFrame(rows, columns=["date", "count"])
    finally:
        session.close()


def get_job_listings(
    role_category: Optional[str] = None,
    seniority: Optional[str] = None,
    is_remote: Optional[bool] = None,
    limit: int = 200,
) -> pd.DataFrame:
    session = _session()
    try:
        q = (session.query(
                JobPosting.title, JobPosting.company, JobPosting.location,
                JobPosting.is_remote, JobPosting.salary_min, JobPosting.salary_max,
                JobPosting.url, JobPosting.posted_date,
                ProcessedJob.role_category, ProcessedJob.seniority,
                ProcessedJob.skills, ProcessedJob.tools,
             )
             .join(ProcessedJob, ProcessedJob.posting_id == JobPosting.id))
        if role_category and role_category != "All":
            q = q.filter(ProcessedJob.role_category == role_category)
        if seniority and seniority != "All":
            q = q.filter(ProcessedJob.seniority == seniority)
        if is_remote is not None:
            q = q.filter(JobPosting.is_remote == is_remote)
        rows = q.order_by(JobPosting.scraped_at.desc()).limit(limit).all()
        return pd.DataFrame(rows, columns=[
            "title","company","location","is_remote","salary_min","salary_max",
            "url","posted_date","role_category","seniority","skills","tools",
        ])
    finally:
        session.close()
