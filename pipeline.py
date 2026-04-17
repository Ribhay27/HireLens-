"""
HireLens – Data Pipeline
Orchestrates: scrape → clean → store → NLP → aggregate
"""

import os
from datetime import datetime
from typing import List, Optional

import pandas as pd
from loguru import logger
from tqdm import tqdm

from models import (
    JobPosting, ProcessedJob, SkillTrend, PipelineRun,
    get_db_session, init_db, get_engine,
)
from processor import NLPProcessor
from indeed_scraper import IndeedScraper, RawJob, DEFAULT_QUERIES, DEFAULT_LOCATIONS


class HireLensPipeline:
    def __init__(
        self,
        db_url: Optional[str] = None,
        queries: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        max_per_query: int = int(os.getenv("MAX_JOBS_PER_RUN", 50)),
        use_spacy: bool = True,
    ):
        self.engine = get_engine(db_url)
        self.max_per_query = max_per_query
        self.queries = queries
        self.locations = locations
        self.nlp = NLPProcessor(use_spacy=use_spacy)
        init_db(self.engine)
        logger.info("HireLensPipeline initialised.")

    def run(self) -> PipelineRun:
        run = self._start_run()
        try:
            raw_jobs = self._scrape_jobs()
            run.jobs_scraped = len(raw_jobs)
            new_ids = self._ingest_raw(raw_jobs)
            processed, errors = self._process_jobs(new_ids)
            run.jobs_processed = processed
            run.errors = errors
            self._update_skill_trends()
            run.status = "success"
            logger.info("Pipeline completed successfully.")
        except Exception as e:
            run.status = "failed"
            run.errors = (run.errors or 0) + 1
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            run.finished_at = datetime.utcnow()
            self._save_run(run)
        return run

    def run_nlp_only(self):
        session = get_db_session(self.engine)
        try:
            ids = [r[0] for r in session.query(JobPosting.id).filter(JobPosting.is_processed == False).all()]
            logger.info(f"Re-processing {len(ids)} unprocessed jobs.")
            self._process_jobs(ids)
            self._update_skill_trends()
        finally:
            session.close()

    def _start_run(self) -> PipelineRun:
        run = PipelineRun(started_at=datetime.utcnow(), status="running")
        session = get_db_session(self.engine)
        try:
            session.add(run)
            session.commit()
            session.refresh(run)
            return run
        finally:
            session.close()

    def _save_run(self, run: PipelineRun):
        session = get_db_session(self.engine)
        try:
            db_run = session.get(PipelineRun, run.id)
            if db_run:
                db_run.finished_at = run.finished_at
                db_run.jobs_scraped = run.jobs_scraped
                db_run.jobs_processed = run.jobs_processed
                db_run.errors = run.errors
                db_run.status = run.status
                session.commit()
        finally:
            session.close()

    def _scrape_jobs(self) -> List[RawJob]:
        raw_jobs = []
        with IndeedScraper() as scraper:
            for job in tqdm(
                scraper.scrape_all_roles(queries=self.queries, locations=self.locations,
                                         max_per_query=self.max_per_query),
                desc="Scraping Indeed", unit="job",
            ):
                raw_jobs.append(job)
        return raw_jobs

    def _ingest_raw(self, raw_jobs: List[RawJob]) -> List[int]:
        session = get_db_session(self.engine)
        try:
            existing = {r[0] for r in session.query(JobPosting.external_id).all()}
            for job in raw_jobs:
                if job.external_id in existing:
                    continue
                session.add(JobPosting(
                    external_id=job.external_id, source=job.source, title=job.title,
                    company=job.company, location=job.location, is_remote=job.is_remote,
                    salary_min=job.salary_min, salary_max=job.salary_max,
                    description_raw=job.description_raw, url=job.url,
                    posted_date=job.posted_date, scraped_at=job.scraped_at,
                ))
                existing.add(job.external_id)
            session.commit()
            return [r[0] for r in session.query(JobPosting.id).filter(JobPosting.is_processed == False).all()]
        finally:
            session.close()

    def _process_jobs(self, job_ids: List[int]) -> tuple:
        if not job_ids:
            return 0, 0
        session = get_db_session(self.engine)
        processed = errors = 0
        try:
            for posting_id in tqdm(job_ids, desc="NLP Processing", unit="job"):
                try:
                    posting = session.get(JobPosting, posting_id)
                    if not posting:
                        continue
                    result = self.nlp.process(posting.title, posting.description_raw or "")
                    session.add(ProcessedJob(
                        posting_id=posting.id,
                        role_category=result["role_category"],
                        seniority=result["seniority"],
                        skills=result["skills"],
                        tools=result["tools"],
                        description_clean=result["description_clean"],
                    ))
                    posting.is_processed = True
                    processed += 1
                    if processed % 50 == 0:
                        session.commit()
                except Exception as e:
                    logger.warning(f"NLP error for posting {posting_id}: {e}")
                    errors += 1
            session.commit()
        finally:
            session.close()
        return processed, errors

    def _update_skill_trends(self):
        session = get_db_session(self.engine)
        try:
            rows = session.query(ProcessedJob.skills, ProcessedJob.tools, ProcessedJob.role_category).all()
            if not rows:
                return
            records = []
            for skills, tools, cat in rows:
                for s in list(skills or []) + list(tools or []):
                    records.append({"skill": s, "category": cat})
            df = pd.DataFrame(records)
            if df.empty:
                return
            session.query(SkillTrend).delete()
            for skill, count in df.groupby("skill").size().items():
                session.add(SkillTrend(skill=skill, category="All", count=int(count)))
            for (skill, cat), count in df.groupby(["skill", "category"]).size().items():
                session.add(SkillTrend(skill=skill, category=cat, count=int(count)))
            session.commit()
            logger.info(f"Skill trends updated.")
        finally:
            session.close()
