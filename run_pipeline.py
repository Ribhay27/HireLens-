"""
HireLens CLI Pipeline Runner

Usage:
    python run_pipeline.py              # Full scrape + NLP + aggregation
    python run_pipeline.py --nlp-only   # Re-run NLP on existing postings
    python run_pipeline.py --init-db    # Initialise DB schema only
    python run_pipeline.py --schedule   # Run every 6 hours
    python run_pipeline.py --schedule --interval 12
"""

import argparse
import sys
import time

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")


def run_full_pipeline():
    from pipeline import HireLensPipeline
    logger.info("Starting full HireLens pipeline...")
    run = HireLensPipeline().run()
    logger.info(f"Pipeline #{run.id} done: status={run.status}, "
                f"scraped={run.jobs_scraped}, processed={run.jobs_processed}, errors={run.errors}")
    return run


def run_nlp_only():
    from pipeline import HireLensPipeline
    logger.info("Running NLP-only pass...")
    HireLensPipeline().run_nlp_only()


def init_db_only():
    from models import init_db, check_connection
    if not check_connection():
        logger.error("Cannot connect to database. Check DATABASE_URL in .env")
        sys.exit(1)
    init_db()
    logger.info("Database schema initialised successfully.")


def run_scheduled(interval_hours=6.0):
    import schedule
    logger.info(f"Scheduling pipeline every {interval_hours} hours.")
    schedule.every(interval_hours).hours.do(run_full_pipeline)
    run_full_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HireLens Pipeline Runner")
    parser.add_argument("--nlp-only",  action="store_true")
    parser.add_argument("--init-db",   action="store_true")
    parser.add_argument("--schedule",  action="store_true")
    parser.add_argument("--interval",  type=float, default=6.0)
    args = parser.parse_args()

    if args.init_db:
        init_db_only()
    elif args.nlp_only:
        run_nlp_only()
    elif args.schedule:
        run_scheduled(args.interval)
    else:
        run_full_pipeline()
