"""
HireLens – NLP Processor
Skill extraction, role classification, seniority detection.
"""

import re
from typing import Dict, List, Optional, Tuple

from loguru import logger

PROGRAMMING_LANGUAGES = {
    "Python", "R", "SQL", "Scala", "Java", "Go", "Rust", "Julia",
    "JavaScript", "TypeScript", "Bash", "Shell", "MATLAB", "SAS", "SPSS", "C++", "C#",
}

DATA_TOOLS = {
    "Airflow", "Prefect", "Dagster", "Luigi", "dbt", "dbt Cloud",
    "Kafka", "Spark", "Flink", "Kinesis", "Pulsar",
    "Snowflake", "BigQuery", "Redshift", "Databricks", "DuckDB",
    "PostgreSQL", "MySQL", "MongoDB", "Cassandra", "Redis", "Elasticsearch",
    "Hive", "Presto", "Trino", "ClickHouse",
    "Fivetran", "Stitch", "Airbyte", "Glue", "Talend",
    "Tableau", "Power BI", "Looker", "Superset", "Metabase", "Grafana", "Qlik",
}

ML_TOOLS = {
    "TensorFlow", "PyTorch", "Keras", "scikit-learn", "XGBoost", "LightGBM",
    "CatBoost", "Hugging Face", "Transformers", "JAX",
    "MLflow", "Kubeflow", "SageMaker", "Vertex AI", "BentoML",
    "Weights & Biases", "W&B", "Neptune",
    "Pandas", "NumPy", "Polars", "Dask", "Ray",
}

CLOUD_PLATFORMS = {
    "AWS", "Azure", "GCP", "Google Cloud", "Kubernetes", "Docker",
    "Terraform", "Ansible", "CloudFormation", "Helm",
    "Lambda", "EC2", "S3", "ECS", "EKS", "EMR",
}

SOFT_SKILLS = {
    "Communication", "Collaboration", "Leadership",
    "Stakeholder Management", "Project Management", "Agile", "Scrum",
}

SKILL_TAXONOMY: Dict[str, str] = {}
for s in PROGRAMMING_LANGUAGES: SKILL_TAXONOMY[s.lower()] = "Programming Language"
for s in DATA_TOOLS:            SKILL_TAXONOMY[s.lower()] = "Data Tool"
for s in ML_TOOLS:              SKILL_TAXONOMY[s.lower()] = "ML / AI"
for s in CLOUD_PLATFORMS:       SKILL_TAXONOMY[s.lower()] = "Cloud / Infra"
for s in SOFT_SKILLS:           SKILL_TAXONOMY[s.lower()] = "Soft Skill"

CANONICAL: Dict[str, str] = {s.lower(): s for s in (
    PROGRAMMING_LANGUAGES | DATA_TOOLS | ML_TOOLS | CLOUD_PLATFORMS | SOFT_SKILLS
)}

ROLE_PATTERNS: List[Tuple[str, List[str]]] = [
    ("Data Engineer",           ["data engineer", "etl developer", "elt developer"]),
    ("Analytics Engineer",      ["analytics engineer", "dbt", "semantic layer"]),
    ("Data Scientist",          ["data scientist", "machine learning scientist", "research scientist"]),
    ("ML Engineer",             ["machine learning engineer", "ml engineer", "mlops", "ai engineer"]),
    ("Data Analyst",            ["data analyst", "business analyst", "reporting analyst"]),
    ("BI Developer",            ["bi developer", "business intelligence", "tableau developer"]),
    ("AI/LLM Engineer",         ["llm", "large language model", "generative ai", "gen ai", "prompt engineer"]),
    ("Platform/Infra Engineer", ["platform engineer", "data platform", "infrastructure engineer", "devops"]),
    ("Data Architect",          ["data architect", "solutions architect"]),
    ("Data Manager",            ["data manager", "head of data", "chief data", "vp of data", "director of data"]),
    ("Data Governance",         ["data governance", "data steward", "data quality", "data catalog"]),
    ("Cloud/DevOps Engineer",   ["cloud engineer", "devops engineer", "site reliability", "sre", "devsecops"]),
    ("Quantitative Analyst",    ["quantitative analyst", "quant analyst", "quant researcher", "quant developer"]),
    ("Product Analyst",         ["product analyst", "growth analyst", "user analytics", "product data"]),
    ("Research Scientist",      ["research scientist", "applied scientist", "ai research", "ml research"]),
]

SENIORITY_PATTERNS: List[Tuple[str, List[str]]] = [
    ("Intern",   ["intern", "internship", "co-op"]),
    ("Junior",   ["junior", "entry level", "entry-level", "associate", "graduate", "jr."]),
    ("Mid",      ["mid-level", "mid level", "intermediate"]),
    ("Senior",   ["senior", "sr.", "sr "]),
    ("Lead",     ["lead", "principal", "staff", "architect"]),
    ("Manager",  ["manager", "director", "head of", "vp ", "vice president", "chief"]),
]


class NLPProcessor:
    def __init__(self, use_spacy: bool = True, spacy_model: str = "en_core_web_sm"):
        self.nlp = None
        if use_spacy:
            try:
                import spacy
                self.nlp = spacy.load(spacy_model)
                logger.info(f"spaCy model '{spacy_model}' loaded.")
            except Exception as e:
                logger.warning(f"spaCy unavailable ({e}). Using keyword matching only.")

    def process(self, title: str, description: str) -> Dict:
        text_lower = f"{title}\n{description}".lower()
        skills, tools = self._extract_skills(text_lower)
        return {
            "skills": skills,
            "tools": tools,
            "role_category": self._classify_role(title, text_lower),
            "seniority": self._classify_seniority(title, text_lower),
            "description_clean": self._clean_text(description),
        }

    def _clean_text(self, text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"https?://\S+", "", text)
        return re.sub(r"\s+", " ", text).strip()

    def _extract_skills(self, text_lower: str) -> Tuple[List[str], List[str]]:
        skills, tools = [], []
        for canonical_lower, display in CANONICAL.items():
            pattern = r"(?<![a-zA-Z0-9])" + re.escape(canonical_lower) + r"(?![a-zA-Z0-9])"
            if re.search(pattern, text_lower):
                cat = SKILL_TAXONOMY.get(canonical_lower, "Other")
                if cat in ("Data Tool", "ML / AI", "Cloud / Infra"):
                    tools.append(display)
                else:
                    skills.append(display)
        return sorted(set(skills)), sorted(set(tools))

    def _classify_role(self, title: str, text_lower: str) -> str:
        title_lower = title.lower()
        for category, keywords in ROLE_PATTERNS:
            for kw in keywords:
                if kw in title_lower:
                    return category
        for category, keywords in ROLE_PATTERNS:
            for kw in keywords:
                if kw in text_lower:
                    return category
        return "Other Tech Role"

    def _classify_seniority(self, title: str, text_lower: str) -> str:
        title_lower = title.lower()
        for level, keywords in SENIORITY_PATTERNS:
            for kw in keywords:
                if kw in title_lower:
                    return level
        for level, keywords in SENIORITY_PATTERNS:
            for kw in keywords:
                if kw in text_lower:
                    return level
        return "Mid"
