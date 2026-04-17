"""
HireLens – Mock Data Seeder
Seeds the database with realistic sample data for development/testing.
Run with: python seed_mock_data.py
          python seed_mock_data.py --n 500
"""

import random
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from models import init_db, get_db_session, get_engine, JobPosting, ProcessedJob, SkillTrend
from processor import NLPProcessor

COMPANIES = [
    "Stripe", "Airbnb", "Netflix", "Uber", "Lyft", "Databricks", "Snowflake",
    "dbt Labs", "Confluent", "HashiCorp", "MongoDB", "Elastic", "Grafana Labs",
    "Scale AI", "Weights & Biases", "Hugging Face", "OpenAI", "Anthropic",
    "Meta", "Google", "Amazon", "Microsoft", "Apple", "Spotify", "Shopify",
    "Palantir", "Two Sigma", "Citadel", "Jane Street", "Square", "Block",
]

LOCATIONS = [
    "San Francisco, CA", "New York, NY", "Seattle, WA", "Austin, TX",
    "Boston, MA", "Chicago, IL", "Los Angeles, CA", "Denver, CO",
    "Remote", "Remote (US)", "Hybrid - San Francisco, CA",
]

JOB_TEMPLATES = [
    {
        "title_prefix": "Senior Data Engineer",
        "description": """We are looking for a Senior Data Engineer to join our data platform team.
You will design and build robust ETL/ELT pipelines using Python, Airflow, and dbt.
Experience with Snowflake or BigQuery is required. You'll work closely with Data Scientists
and Analytics Engineers to ensure high-quality, reliable data infrastructure.
Requirements: Python, SQL, Apache Airflow, dbt, Snowflake or BigQuery, Spark,
Docker, Kubernetes, AWS or GCP. 5+ years of experience building production data pipelines.""",
    },
    {
        "title_prefix": "Data Scientist",
        "description": """Join our ML team to develop predictive models that power our core product.
You'll work on customer churn prediction, demand forecasting, and recommendation systems
using Python, scikit-learn, and PyTorch. Experience with MLflow for experiment tracking
and SageMaker or Vertex AI for model deployment preferred.
Skills: Python, R, TensorFlow or PyTorch, scikit-learn, SQL, pandas, NumPy, statistics, A/B testing.""",
    },
    {
        "title_prefix": "Machine Learning Engineer",
        "description": """We're building production ML systems at scale. As an ML Engineer you'll
own the full ML lifecycle: training, evaluation, deployment, and monitoring.
Stack includes PyTorch, Kubeflow, Kafka for real-time inference, and AWS for infrastructure.
Requirements: Python, PyTorch or TensorFlow, MLflow, Docker, Kubernetes,
AWS or GCP, Kafka or Kinesis, Spark, 3+ years ML engineering experience.""",
    },
    {
        "title_prefix": "Analytics Engineer",
        "description": """As an Analytics Engineer you'll sit between data engineering and analytics,
owning our dbt project and semantic layer. You'll transform raw data in Snowflake into
clean, documented, tested data models consumed by Tableau and Looker dashboards.
Skills: dbt, SQL, Snowflake or BigQuery or Redshift, Python, Tableau or Looker, git, data modeling.""",
    },
    {
        "title_prefix": "Data Analyst",
        "description": """We're looking for a data-driven analyst to support our growth and product teams.
You'll build dashboards in Tableau and Power BI, write complex SQL queries against our
BigQuery warehouse, and present findings to senior stakeholders.
Skills: SQL, Excel, Tableau or Power BI, Python, pandas, BigQuery or Snowflake,
communication, A/B testing, business intelligence.""",
    },
    {
        "title_prefix": "AI/LLM Engineer",
        "description": """We're building LLM-powered products and need an engineer with hands-on
experience fine-tuning and deploying large language models. You'll work with
Hugging Face Transformers, LangChain, and AWS to build RAG pipelines and agentic workflows.
Skills: Python, PyTorch, Hugging Face, AWS, Docker, Redis, Elasticsearch, prompt engineering.""",
    },
    {
        "title_prefix": "Junior Data Engineer",
        "description": """Great entry-level opportunity for a data engineer to join a growing team.
You'll assist in building and maintaining data pipelines using Python and Airflow,
write SQL queries, and help migrate workloads to our Databricks lakehouse.
Skills: Python, SQL, basic ETL knowledge, familiarity with AWS or GCP, willingness to learn Airflow and dbt.""",
    },
    {
        "title_prefix": "Staff Data Engineer",
        "description": """As a Staff Data Engineer you'll set technical direction for our data platform,
mentor engineers, and lead cross-functional initiatives. You'll drive adoption of
modern data stack tooling: dbt, Dagster, Snowflake, and Kafka.
Skills: Python, Scala or Java, Kafka, Spark, Flink, dbt, Snowflake, Kubernetes, Terraform, AWS, 8+ years.""",
    },
    {
        "title_prefix": "Data Governance Analyst",
        "description": """We're hiring a Data Governance Analyst to own our data quality and
data catalog initiatives. You'll define data standards, manage metadata in Collibra,
and work with engineering teams to implement data quality checks using Great Expectations.
Skills: SQL, Python, Collibra or Alation, Great Expectations, data lineage, data quality, stakeholder management.""",
    },
    {
        "title_prefix": "Cloud DevOps Engineer",
        "description": """Join our platform team as a Cloud DevOps Engineer. You'll build and
maintain our cloud infrastructure on AWS, manage Kubernetes clusters, and build CI/CD pipelines.
You'll work closely with data and ML teams to ensure reliable, scalable infrastructure.
Skills: AWS or GCP or Azure, Kubernetes, Docker, Terraform, Ansible, CI/CD, Python, Bash, monitoring.""",
    },
    {
        "title_prefix": "Quantitative Analyst",
        "description": """We are looking for a Quantitative Analyst to join our trading team.
You'll develop mathematical models for risk management and trading strategy,
backtest strategies using Python, and work with large financial datasets.
Skills: Python, R, SQL, pandas, NumPy, statistics, probability, linear algebra, financial modeling, Excel.""",
    },
    {
        "title_prefix": "Product Analyst",
        "description": """As a Product Analyst you'll partner with our product managers and engineers
to drive data-informed product decisions. You'll run A/B tests, build funnels in Mixpanel,
and write SQL to answer questions about user behaviour.
Skills: SQL, Python, A/B testing, Mixpanel or Amplitude, Tableau or Looker, statistics, product sense.""",
    },
    {
        "title_prefix": "Research Scientist",
        "description": """We're looking for a Research Scientist to work on cutting-edge AI problems.
You'll publish research, develop novel ML algorithms, and collaborate with engineering
to bring research into production. Strong background in deep learning required.
Skills: Python, PyTorch or JAX, deep learning, NLP or computer vision, publications, PhD preferred.""",
    },
]

SALARY_RANGES = {
    "Intern":   (45_000,  75_000),
    "Junior":   (70_000,  100_000),
    "Mid":      (100_000, 145_000),
    "Senior":   (140_000, 200_000),
    "Lead":     (170_000, 240_000),
    "Staff":    (200_000, 280_000),
}


def seed(n: int = 300):
    engine = get_engine()
    init_db(engine)
    session = get_db_session(engine)
    nlp = NLPProcessor(use_spacy=False)

    logger.info(f"Seeding {n} mock job postings...")

    for i in range(n):
        template = random.choice(JOB_TEMPLATES)
        company = random.choice(COMPANIES)
        location = random.choice(LOCATIONS)
        is_remote = "remote" in location.lower()

        seniority_prefix = random.choice(["", "Senior ", "Junior ", "Staff ", "Lead ", "Principal "])
        title = f"{seniority_prefix}{template['title_prefix']}".strip()

        sal_key = ("Senior" if "Senior" in title else
                   "Junior" if "Junior" in title else
                   "Lead"   if any(w in title for w in ["Lead","Staff","Principal"]) else "Mid")
        sal_min, sal_max = SALARY_RANGES[sal_key]
        sal_min += random.randint(-10_000, 10_000)
        sal_max += random.randint(-10_000, 20_000)
        include_salary = random.random() > 0.55

        posted = datetime.utcnow() - timedelta(days=random.randint(0, 28))

        session.add(JobPosting(
            external_id=f"mock_{i:05d}",
            source="mock",
            title=title,
            company=company,
            location=location,
            is_remote=is_remote,
            salary_min=sal_min if include_salary else None,
            salary_max=sal_max if include_salary else None,
            description_raw=template["description"],
            url=f"https://indeed.com/mock/{i}",
            posted_date=posted,
            is_processed=False,
        ))

    session.commit()
    logger.info("Mock postings inserted. Running NLP processing...")

    postings = session.query(JobPosting).filter(JobPosting.is_processed == False).all()
    for posting in postings:
        result = nlp.process(posting.title, posting.description_raw or "")
        session.add(ProcessedJob(
            posting_id=posting.id,
            role_category=result["role_category"],
            seniority=result["seniority"],
            skills=result["skills"],
            tools=result["tools"],
            description_clean=result["description_clean"],
        ))
        posting.is_processed = True

    session.commit()
    logger.info("NLP done. Aggregating skill trends...")

    rows = session.query(ProcessedJob.skills, ProcessedJob.tools, ProcessedJob.role_category).all()
    records = []
    for skills, tools, cat in rows:
        for s in list(skills or []) + list(tools or []):
            records.append({"skill": s, "category": cat})

    if records:
        df = pd.DataFrame(records)
        session.query(SkillTrend).delete()
        for skill, count in df.groupby("skill").size().items():
            session.add(SkillTrend(skill=skill, category="All", count=int(count)))
        for (skill, cat), count in df.groupby(["skill", "category"]).size().items():
            session.add(SkillTrend(skill=skill, category=cat, count=int(count)))
        session.commit()

    session.close()
    logger.info(f"✅ Seeded {n} jobs. Now run: streamlit run app.py")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=300)
    args = parser.parse_args()
    seed(args.n)
