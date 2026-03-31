"""
HireLens – NLP Processor
Extracts skills, tools, seniority, and role categories from job descriptions.

Uses spaCy for NLP + curated skill/tool taxonomy for extraction.
"""

import re
from typing import Dict, List, Optional, Tuple

from loguru import logger

# ── Skill Taxonomy ─────────────────────────────────────────────────────────────
# Organised by category for richer analytics

PROGRAMMING_LANGUAGES = {
    "Python", "R", "SQL", "Scala", "Java", "Go", "Rust", "Julia",
    "JavaScript", "TypeScript", "Bash", "Shell", "MATLAB", "SAS", "SPSS",
    "C++", "C#", "Kotlin", "Swift",
}

DATA_TOOLS = {
    # Orchestration
    "Airflow", "Prefect", "Dagster", "Luigi", "dbt", "dbt Cloud",
    # Streaming
    "Kafka", "Spark", "Flink", "Kinesis", "Pulsar",
    # Warehouses / Databases
    "Snowflake", "BigQuery", "Redshift", "Databricks", "DuckDB",
    "PostgreSQL", "MySQL", "MongoDB", "Cassandra", "Redis", "Elasticsearch",
    "Hive", "Presto", "Trino", "ClickHouse",
    # Ingestion / ELT
    "Fivetran", "Stitch", "Airbyte", "Glue", "Talend",
    # BI / Visualisation
    "Tableau", "Power BI", "Looker", "Superset", "Metabase", "Grafana",
    "Qlik", "MicroStrategy",
}

ML_TOOLS = {
    # Frameworks
    "TensorFlow", "PyTorch", "Keras", "scikit-learn", "XGBoost", "LightGBM",
    "CatBoost", "Hugging Face", "Transformers", "JAX",
    # MLOps
    "MLflow", "Kubeflow", "SageMaker", "Vertex AI", "BentoML",
    "Seldon", "Feast", "Weights & Biases", "W&B", "Neptune",
    # Data
    "Pandas", "NumPy", "Polars", "Dask", "Ray",
}

CLOUD_PLATFORMS = {
    "AWS", "Azure", "GCP", "Google Cloud", "Kubernetes", "Docker",
    "Terraform", "Ansible", "CloudFormation", "Helm",
    "Lambda", "EC2", "S3", "ECS", "EKS", "EMR",
}

SOFT_SKILLS = {
    "Communication", "Collaboration", "Problem-solving", "Leadership",
    "Stakeholder Management", "Project Management", "Agile", "Scrum",
}

# Combined lookup: skill → category
SKILL_TAXONOMY: Dict[str, str] = {}
for skill in PROGRAMMING_LANGUAGES:
    SKILL_TAXONOMY[skill.lower()] = "Programming Language"
for skill in DATA_TOOLS:
    SKILL_TAXONOMY[skill.lower()] = "Data Tool"
for skill in ML_TOOLS:
    SKILL_TAXONOMY[skill.lower()] = "ML / AI"
for skill in CLOUD_PLATFORMS:
    SKILL_TAXONOMY[skill.lower()] = "Cloud / Infra"
for skill in SOFT_SKILLS:
    SKILL_TAXONOMY[skill.lower()] = "Soft Skill"

# Canonical capitalisation map (lower → display)
CANONICAL: Dict[str, str] = {s.lower(): s for s in (
    PROGRAMMING_LANGUAGES | DATA_TOOLS | ML_TOOLS | CLOUD_PLATFORMS | SOFT_SKILLS
)}

# ── Role Category Taxonomy ─────────────────────────────────────────────────────

ROLE_PATTERNS: List[Tuple[str, List[str]]] = [
    ("Data Engineer",          ["data engineer", "data pipeline", "etl developer", "elt developer"]),
    ("Analytics Engineer",     ["analytics engineer", "dbt", "semantic layer"]),
    ("Data Scientist",         ["data scientist", "machine learning scientist", "research scientist"]),
    ("ML Engineer",            ["machine learning engineer", "ml engineer", "mlops", "ai engineer"]),
    ("Data Analyst",           ["data analyst", "business analyst", "reporting analyst"]),
    ("BI Developer",           ["bi developer", "business intelligence", "tableau developer", "power bi"]),
    ("AI/LLM Engineer",        ["llm", "large language model", "generative ai", "gen ai", "prompt engineer"]),
    ("Platform/Infra Engineer",["platform engineer", "data platform", "infrastructure engineer", "devops"]),
    ("Data Architect",         ["data architect", "solutions architect", "enterprise architect"]),
    ("Data Manager",           ["data manager", "head of data", "chief data", "vp of data", "director of data"]),
]

SENIORITY_PATTERNS: List[Tuple[str, List[str]]] = [
    ("Intern",   ["intern", "internship", "co-op", "coop"]),
    ("Junior",   ["junior", "entry level", "entry-level", "associate", "graduate", "jr."]),
    ("Mid",      ["mid-level", "mid level", "intermediate", "ii ", " 2 "]),
    ("Senior",   ["senior", "sr.", "sr "]),
    ("Lead",     ["lead", "principal", "staff", "architect"]),
    ("Manager",  ["manager", "director", "head of", "vp ", "vice president", "chief"]),
]


# ── Extractor ──────────────────────────────────────────────────────────────────

class NLPProcessor:
    """
    Extracts structured signals from raw job descriptions.

    Optionally loads spaCy for named entity recognition; falls back
    to pure regex/keyword matching if spaCy is not installed.
    """

    def __init__(self, use_spacy: bool = True, spacy_model: str = "en_core_web_sm"):
        self.nlp = None
        if use_spacy:
            try:
                import spacy
                self.nlp = spacy.load(spacy_model)
                logger.info(f"spaCy model '{spacy_model}' loaded.")
            except Exception as e:
                logger.warning(f"spaCy unavailable ({e}). Falling back to keyword matching.")

    # ── Public API ─────────────────────────────────────────────────────────────

    def process(self, title: str, description: str) -> Dict:
        """
        Full processing pipeline for a single job posting.

        Returns:
            dict with keys: skills, tools, role_category, seniority, description_clean
        """
        text = f"{title}\n{description}"
        clean = self._clean_text(description)
        text_lower = text.lower()

        skills, tools = self._extract_skills(text_lower)
        role_category = self._classify_role(title, text_lower)
        seniority = self._classify_seniority(title, text_lower)

        return {
            "skills": skills,
            "tools": tools,
            "role_category": role_category,
            "seniority": seniority,
            "description_clean": clean,
        }

    def extract_skills(self, text: str) -> List[str]:
        """Return deduplicated list of all recognised skills + tools."""
        skills, tools = self._extract_skills(text.lower())
        return list(set(skills + tools))

    def classify_role(self, title: str, description: str = "") -> str:
        return self._classify_role(title, f"{title} {description}".lower())

    def classify_seniority(self, title: str, description: str = "") -> str:
        return self._classify_seniority(title, f"{title} {description}".lower())

    # ── Private ────────────────────────────────────────────────────────────────

    def _clean_text(self, text: str) -> str:
        """Basic text cleaning: strip HTML, normalise whitespace."""
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Remove URLs
        text = re.sub(r"https?://\S+", "", text)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _extract_skills(self, text_lower: str) -> Tuple[List[str], List[str]]:
        """
        Match skills from the taxonomy using whole-word regex.
        Returns (skills_list, tools_list) — both de-duped and in canonical form.
        """
        found_skills = []
        found_tools = []

        for canonical_lower, display in CANONICAL.items():
            # Use word boundary matching; handle special chars in tool names
            pattern = r"(?<![a-zA-Z0-9])" + re.escape(canonical_lower) + r"(?![a-zA-Z0-9])"
            if re.search(pattern, text_lower):
                category = SKILL_TAXONOMY.get(canonical_lower, "Other")
                if category in ("Data Tool", "ML / AI", "Cloud / Infra"):
                    found_tools.append(display)
                else:
                    found_skills.append(display)

        return sorted(set(found_skills)), sorted(set(found_tools))

    def _classify_role(self, title: str, text_lower: str) -> str:
        """Match job title → role category."""
        title_lower = title.lower()
        # Title-first matching
        for category, keywords in ROLE_PATTERNS:
            for kw in keywords:
                if kw in title_lower:
                    return category
        # Fallback: description keyword matching
        for category, keywords in ROLE_PATTERNS:
            for kw in keywords:
                if kw in text_lower:
                    return category
        return "Other Tech Role"

    def _classify_seniority(self, title: str, text_lower: str) -> str:
        """Match seniority level from title then description."""
        title_lower = title.lower()
        for level, keywords in SENIORITY_PATTERNS:
            for kw in keywords:
                if kw in title_lower:
                    return level
        for level, keywords in SENIORITY_PATTERNS:
            for kw in keywords:
                if kw in text_lower:
                    return level
        return "Mid"   # default assumption

    def batch_process(self, jobs: List[Dict]) -> List[Dict]:
        """
        Process a list of job dicts (with 'title' and 'description_raw' keys).
        Returns list of result dicts merged with original data.
        """
        results = []
        for job in jobs:
            try:
                processed = self.process(
                    title=job.get("title", ""),
                    description=job.get("description_raw", ""),
                )
                results.append({**job, **processed})
            except Exception as e:
                logger.warning(f"NLP error for job '{job.get('title')}': {e}")
                results.append(job)
        return results
