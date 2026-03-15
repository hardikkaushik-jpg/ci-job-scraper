# clean_jobs_cplus.py — Refactored v5.0
# Changes:
#   1. Reads Description column and uses it in relevancy scoring
#   2. Databricks added to company group mapping
#   3. Scoring calibrated — removed double-multiplier bug
#   4. first_seen / last_seen passed through
#   5. Cleaner field ordering

import re
import json
import csv
import os

# ─────────────────────────────────────────────────────────────────────────────
# SKILL MAPPING
# ─────────────────────────────────────────────────────────────────────────────
_SKILL_CANON = {
    "python": "PYTHON", "python3": "PYTHON",
    "sql": "SQL", "tsql": "SQL",
    "postgres": "POSTGRES", "postgresql": "POSTGRES",
    "aws": "AWS", "gcp": "GCP", "azure": "AZURE",
    "spark": "SPARK", "kafka": "KAFKA",
    "dbt": "DBT", "airflow": "AIRFLOW",
    "etl": "ETL", "elt": "ELT",
    "java": "JAVA", "scala": "SCALA",
    "go": "GO", "rust": "RUST",
    "javascript": "JAVASCRIPT", "js": "JAVASCRIPT",
    "hive": "HIVE",
    "snowflake": "SNOWFLAKE",
    "redshift": "REDSHIFT",
    "bigquery": "BIGQUERY",
    "databricks": "DATABRICKS",
    "observability": "OBSERVABILITY",
    "data quality": "DATA_QUALITY",
    "dataops": "DATAOPS",
    "governance": "GOVERNANCE",
    "lineage": "LINEAGE",
    "metadata": "METADATA",
    "ml": "ML", "ai": "AI",
    "streaming": "STREAMING",
    "prometheus": "PROMETHEUS",
    "grafana": "GRAFANA",
    "kubernetes": "KUBERNETES",
    "docker": "DOCKER",
    "terraform": "TERRAFORM",
    # Vector / AI
    "vector": "VECTOR",
    "embedding": "EMBEDDING",
    "rag": "RAG",
    "llm": "LLM",
    "faiss": "FAISS",
    "pinecone": "PINECONE",
    "weaviate": "WEAVIATE",
    "qdrant": "QDRANT",
    "milvus": "MILVUS",
    "chromadb": "CHROMADB",
    "langchain": "LANGCHAIN",
    "semantic search": "SEMANTIC_SEARCH",
    "mlops": "MLOPS",
    "pytorch": "PYTORCH",
    "tensorflow": "TENSORFLOW",
    "hugging face": "HUGGINGFACE",
    "transformers": "TRANSFORMERS",
}

_SKILL_TOKEN_RE = re.compile(r'\b([A-Za-z0-9+#\.\-/]{1,40})\b')

# ─────────────────────────────────────────────────────────────────────────────
# COMPANY GROUPS  (expanded + Databricks fixed)
# ─────────────────────────────────────────────────────────────────────────────
_COMPANY_GROUPS = {
    "Data Intelligence": {
        "collibra", "informatica", "atlan", "alation",
        "datagalaxy", "pentaho", "data.world", "solidatus",
    },
    "Data Observability": {
        "acceldata", "anomalo", "bigeye", "monte carlo",
        "sifflet", "decube", "metaplane",
    },
    "ETL/Connectors": {
        "fivetran", "airbyte", "talend", "matillion",
        "snaplogic", "boomi", "syniti", "precisely",
        "informatica",
    },
    "Warehouse/Processing": {
        "snowflake", "databricks", "amazon", "redshift",
        "bigquery", "teradata", "vertica", "exasol",
        "firebolt", "influxdata", "yellowbrick",
    },
    "Monitoring/Platforms": {
        "datadog", "splunk", "new relic",
    },
    "Vector DB / AI Storage": {
        "pinecone", "weaviate", "qdrant", "zilliz",
        "milvus", "chromadb", "chroma",
    },
    "Other": set(),
}

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT FOCUS
# ─────────────────────────────────────────────────────────────────────────────
_PRODUCT_KEYWORDS = {
    "Data Quality":          ["data quality", "accuracy", "data testing", "data validation"],
    "Data Observability":    ["observability", "monitor", "anomaly", "alert", "telemetry"],
    "Data Governance":       ["governance", "catalog", "glossary", "lineage", "metadata"],
    "ETL/Integration":       ["etl", "integrat", "replicat", "pipeline", "sync", "connector"],
    "Streaming / Real-time": ["stream", "kafka", "real-time", "flink"],
    "ML/AI infra":           ["ml", "machine learning", "mlops", "model", "llm", "ai", "rag"],
    "Platform / Infra":      ["sre", "infra", "platform", "kubernetes", "aws", "gcp", "azure"],
    "Vector / Embedding":    ["vector", "embedding", "similarity search", "faiss", "ann",
                              "rag", "retrieval", "semantic search", "pinecone", "weaviate",
                              "qdrant", "milvus", "chroma"],
}

# ─────────────────────────────────────────────────────────────────────────────
# RELEVANCY SCORING
# ─────────────────────────────────────────────────────────────────────────────
_ACTIAN_RELEVANT_SKILLS = {
    "ETL", "DBT", "SQL", "AWS", "AZURE", "GCP",
    "SNOWFLAKE", "DATABRICKS", "POSTGRES", "STREAMING",
    "KAFKA", "RUST", "GO", "GOVERNANCE", "OBSERVABILITY",
    "LINEAGE", "DATA_QUALITY", "INTEGRATION",
    # Vector/AI — relevant for VectorAI product
    "VECTOR", "EMBEDDING", "RAG", "LLM", "FAISS",
    "PINECONE", "WEAVIATE", "QDRANT", "MILVUS",
    "SEMANTIC_SEARCH", "MLOPS",
}

# Calibrated weights — score is NOT multiplied by 10 anymore, kept 0-100 range
_WEIGHTS = {
    "skill_match":       4.0,   # per matching skill
    "product_relevancy": 6.0,   # for high-relevancy product focus
    "geo_relevancy":     2.0,
    "seniority":         1.0,
    "ai_focus":          2.5,
    "desc_bonus":        3.0,   # bonus when description confirms signals
}

_ACTIAN_GEOS = {
    "united states", "germany", "india", "uk",
    "singapore", "canada", "australia",
}

_SENIORITY_VALUE = {
    "Director+":        1.0,
    "Principal/Staff":  0.9,
    "Senior":           0.7,
    "Manager":          0.6,
    "Mid":              0.4,
    "Entry":            0.2,
    "Intern":           0.05,
    "Unknown":          0.1,
}

HIGH_RELEVANCY_PRODUCTS = {
    "ETL/Integration", "Data Governance",
    "Data Observability", "Streaming / Real-time",
    "Vector / Embedding",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _normalize_skill_token(tok):
    if not tok:
        return None
    return _SKILL_CANON.get(tok.lower().strip(".+-() "))


def extract_skills(title, description=""):
    """Extract skills from title first, then description as secondary signal."""
    text = (title or "") + " " + (description or "")
    if not text.strip():
        return []

    skills = []
    low = text.lower()

    for m in _SKILL_TOKEN_RE.finditer(text):
        norm = _normalize_skill_token(m.group(1))
        if norm and norm not in skills:
            skills.append(norm)

    for phrase, canon in _SKILL_CANON.items():
        if " " in phrase and phrase in low and canon not in skills:
            skills.append(canon)

    return skills[:15]


def classify_company_group(company):
    low = company.lower()
    for group, words in _COMPANY_GROUPS.items():
        if group == "Other":
            continue
        if any(w in low for w in words):
            return group
    return "Other"


def detect_product_focus(title, description=""):
    txt = ((title or "") + " " + (description or "")).lower()
    if not txt.strip():
        return "Other", []
    matches = [
        label for label, kws in _PRODUCT_KEYWORDS.items()
        if any(kw in txt for kw in kws)
    ]
    return (matches[0] if matches else "Other"), matches


def compute_relevancy_to_actian(title, location, description, skills, pfocus, seniority):
    """
    Score 0-100 how relevant a job is to Actian's competitive space.
    Uses title + description when available.
    """
    score = 0.0

    # Skill matches
    matched_skills = [s for s in skills if s in _ACTIAN_RELEVANT_SKILLS]
    score += _WEIGHTS["skill_match"] * len(matched_skills)

    # Product focus
    if pfocus in HIGH_RELEVANCY_PRODUCTS:
        score += _WEIGHTS["product_relevancy"]

    # Geography
    loc_low = location.lower()
    if any(g in loc_low for g in _ACTIAN_GEOS):
        score += _WEIGHTS["geo_relevancy"]

    # Seniority
    score += _WEIGHTS["seniority"] * _SENIORITY_VALUE.get(seniority, 0.1)

    # AI/ML bonus
    if any(x in skills for x in ("AI", "ML", "MLOPS")):
        score += _WEIGHTS["ai_focus"]

    # Description bonus — extra signal when description available
    if description:
        desc_low = description.lower()
        desc_signal_words = [
            "data integration", "etl", "pipeline", "connector",
            "data quality", "governance", "observability", "actian",
        ]
        bonus_hits = sum(1 for w in desc_signal_words if w in desc_low)
        score += _WEIGHTS["desc_bonus"] * min(bonus_hits, 3)

    return min(100.0, round(max(0.0, score), 1))


def compute_trend_score(title, description, seniority):
    text = ((title or "") + " " + (description or "")).lower()
    s = 0.0
    if re.search(r"ai|ml|llm|gpt|rag|mlops", text):    s += 2.0
    if re.search(r"stream|kafka|real-time|flink", text): s += 1.5
    if re.search(r"observab|monitor|anomal", text):      s += 1.5
    if re.search(r"governance|catalog|lineage", text):   s += 1.0
    if seniority in ("Senior", "Principal/Staff", "Director+"):
        s += 1.0
    return min(10.0, round(s, 2))


def infer_function(title):
    t = (title or "").lower()

    # AI/ML and Vector checked FIRST — before generic Engineering catch-all
    # so "ML Engineer", "AI Researcher", "Vector Search Engineer" all land here
    if re.search(
        r"\b(machine learning|ml engineer|ml infra|mlops|llm|large language|"
        r"ai engineer|ai researcher|ai scientist|generative|gen ai|rag|"
        r"vector|embedding|semantic search|similarity|recommendation engine|"
        r"deep learning|nlp|computer vision|model training|model serving|"
        r"data scientist|research scientist|applied scientist)\b", t
    ):
        return "AI/ML & Vector"

    if re.search(r"(engineer|developer|devops|sre|software|architect)", t):
        return "Engineering"
    if "data" in t or "analyt" in t:
        return "Data/Analytics"
    if "product" in t:
        return "Product"
    if "sales" in t or "account" in t or "revenue" in t:
        return "Sales"
    if "marketing" in t:
        return "Marketing"
    if "hr" in t or "talent" in t or "recruit" in t or "people" in t:
        return "People/HR"
    if "ops" in t or "support" in t or "customer" in t:
        return "Operations"
    return "Other"


# ─────────────────────────────────────────────────────────────────────────────
# ROW ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────
def enrich_row(r):
    title       = r.get("Job Title", "")
    location    = r.get("Location", "")
    seniority   = r.get("Seniority", "Unknown")
    description = r.get("Description", "")

    skills = extract_skills(title, description)
    company_group = classify_company_group(r.get("Company", ""))
    pfocus, pf_tokens = detect_product_focus(title, description)

    relevancy = compute_relevancy_to_actian(
        title, location, description, skills, pfocus, seniority
    )
    trend = compute_trend_score(title, description, seniority)

    r["Extracted_Skills"]     = skills
    r["Primary_Skill"]        = skills[0] if skills else ""
    r["Company_Group"]        = company_group
    r["Product_Focus"]        = pfocus
    r["Product_Focus_Tokens"] = pf_tokens
    r["Relevancy_to_Actian"]  = relevancy
    r["Trend_Score"]          = trend
    r["Skills_in_Title"]      = ",".join(extract_skills(title))
    r["Function"]             = infer_function(title)

    return r


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    repo    = os.path.dirname(os.path.abspath(__file__))
    infile  = os.path.join(repo, "jobs_final_hard.csv")
    outfile = os.path.join(repo, "jobs_cleaned_final_enriched.csv")

    rows = []
    with open(infile, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "Company":        r.get("Company", ""),
                "Job Title":      r.get("Job Title", ""),
                "Job Link":       r.get("Job Link", ""),
                "Location":       r.get("Location", ""),
                "Posting Date":   r.get("Posting Date", ""),
                "Days Since Posted": r.get("Days Since Posted", ""),
                "Seniority":      r.get("Seniority", "Unknown"),
                "Description":    r.get("Description", ""),
                "First_Seen":     r.get("First_Seen", ""),
                "Last_Seen":      r.get("Last_Seen", ""),
            })

    enriched = [enrich_row(r) for r in rows]

    # Serialise lists
    for r in enriched:
        r["Extracted_Skills"]     = json.dumps(r["Extracted_Skills"])
        r["Product_Focus_Tokens"] = json.dumps(r["Product_Focus_Tokens"])

    fieldnames = [
        "Company", "Job Title", "Job Link", "Location",
        "Posting Date", "Days Since Posted",
        "Function", "Seniority", "Skills_in_Title",
        "Company_Group", "Product_Focus", "Product_Focus_Tokens",
        "Primary_Skill", "Extracted_Skills",
        "Relevancy_to_Actian", "Trend_Score",
        "First_Seen", "Last_Seen",
        # Description intentionally excluded from enriched CSV
        # (kept in jobs_final_hard.csv for scoring purposes only)
    ]

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in enriched:
            w.writerow({c: r.get(c, "") for c in fieldnames})

    print(f"[CLEANER] Wrote {len(enriched)} enriched rows -> {outfile}")


if __name__ == "__main__":
    main()
