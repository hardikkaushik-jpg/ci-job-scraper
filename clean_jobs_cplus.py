# clean_jobs_cplus.py
# Cleaner / Enricher v3 for jobs pipeline
# Usage: import enrich_rows from this module and call enrich_rows(rows)
# rows is list of dicts as produced by your scraper (Company, Job Title, Job Link, Location, Posting Date, ...)

import re
from datetime import date
from collections import defaultdict

# -------------------------
# Config / lookups
# -------------------------

# canonical skills mapping (lowercase -> canonical token)
_SKILL_CANON = {
    "python": "PYTHON",
    "python3": "PYTHON",
    "py": "PYTHON",
    "r": "R",
    "sql": "SQL",
    "postgres": "POSTGRES",
    "postgresql": "POSTGRES",
    "aws": "AWS",
    "amazon web services": "AWS",
    "gcp": "GCP",
    "google cloud": "GCP",
    "azure": "AZURE",
    "spark": "SPARK",
    "kafka": "KAFKA",
    "dbt": "DBT",
    "airflow": "AIRFLOW",
    "etl": "ETL",
    "elt": "ELT",
    "etl/elt": "ETL",
    "java": "JAVA",
    "scala": "SCALA",
    "go": "GO",
    "golang": "GO",
    "rust": "RUST",
    "javascript": "JAVASCRIPT",
    "js": "JAVASCRIPT",
    "typescript": "JAVASCRIPT",
    "hive": "HIVE",
    "snowflake": "SNOWFLAKE",
    "redshift": "REDSHIFT",
    "bigquery": "BIGQUERY",
    "databricks": "DATABRICKS",
    "observability": "OBSERVABILITY",
    "data quality": "DATA_QUALITY",
    "dataops": "DATAOPS",
    "governance": "GOVERNANCE",
    "data governance": "GOVERNANCE",
    "lineage": "LINEAGE",
    "metadata": "METADATA",
    "ml": "ML",
    "machine learning": "ML",
    "ai": "AI",
    "spark streaming": "STREAMING",
    "streaming": "STREAMING",
    "prometheus": "PROMETHEUS",
    "grafana": "GRAFANA"
    # extend as needed
}

# tokenization regex for skills detection (word boundaries + common characters)
_SKILL_TOKEN_RE = re.compile(r'\b([A-Za-z+#\.\-/]{1,40})\b')

# Competitor grouping (you asked 5-6 buckets). These map company names -> group.
# Tune this to match your competitor list. Case-insensitive keys.
_COMPANY_GROUPS = {
    "data intelligence": {"collibra","informatica","atlan","alation","datagalaxy","pentaho","microsoft purview","alex solutions"},
    "data observability": {"acceldata","anomalo","bigeye","monte carlo"},
    "etl/connectors": {"fivetran","airbyte","talend","matillion","snaplogic","boomi"},
    "warehouse/processing": {"snowflake","databricks","redshift","bigquery","teradata","vertica"},
    "monitoring/platforms": {"datadog","splunk","new relic"},
    # fallback group is 'other'
}

# product focus labels — detect primary area of job posting
_PRODUCT_KEYWORDS = {
    "Data Quality": ["data quality","quality monitoring","data health","accuracy","data testing"],
    "Data Observability": ["observability","data observability","monitoring","anomaly detection","metrics","alerts","slo","sla","telemetry"],
    "Data Governance": ["governance","policy","catalog","glossary","compliance","data catalog","lineage","metadata"],
    "ETL/Integration": ["etl","integrat","connector","replicat","sync","pipeline","data movement","replicate","ingest","connector"],
    "Streaming / Real-time": ["stream","streaming","kafka","pubsub","kinesis","real[- ]time"],
    "ML/AI infra": ["ml","machine learning","mlops","ai","model serving","model monitoring","r&d"],
    "Platform / Infra": ["sre","site reliability","infrastructure","cloud","platform","kubernetes","aws","gcp","azure"]
}

# Skills that increase Relevancy to Actian (example)
# Actian strengths: hybrid architectures, connectors, cloud & on-prem data platform, performance, SQL engines
_ACTIAN_RELEVANT_SKILLS = {"ETL","DBT","SQL","AWS","AZURE","GCP","SNOWFLAKE","DATABRICKS","POSTGRES","STREAMING","KAFKA","RUST","GO"}

# score weights (tweakable)
_WEIGHTS = {
    "skill_relevancy": 1.0,
    "product_relevancy": 1.5,
    "geo_relevancy": 0.6,
    "seniority_relevancy": 0.3,
    "ai_focus": 0.8
}

# geo preferences for Actian — adjust to the company target geos
_ACTIAN_GEOS = {"united states","germany","india","uk","singapore","canada","australia"}

# seniority mapping that matters more to Actian (example)
_SENIORITY_VALUE = {
    "Director+": 1.0,
    "Principal/Staff": 0.9,
    "Senior": 0.7,
    "Manager": 0.6,
    "Mid": 0.4,
    "Entry": 0.2,
    "Intern": 0.05,
    "Unknown": 0.1
}

# -------------------------
# helper functions
# -------------------------

def _normalize_skill_token(tok: str):
    if not tok:
        return None
    low = tok.strip().lower()
    low = low.replace("()", "")
    # direct map
    if low in _SKILL_CANON:
        return _SKILL_CANON[low]
    # punctuation clean
    low_clean = low.strip(".+-")
    if low_clean in _SKILL_CANON:
        return _SKILL_CANON[low_clean]
    # token heuristics
    if re.match(r'^(py(thon)?)(\d)?$', low):
        return "PYTHON"
    if low in ("r",):
        return "R"
    if low in ("sql", "tsql"):
        return "SQL"
    # short heuristics: if token is uppercase-like, return upper
    if len(low) <= 5 and low.isalpha():
        return low.upper()
    return None

def extract_skills_from_text(text, top_n=12):
    """Return list of canonical skill tokens extracted from text (title+desc)."""
    if not text:
        return []
    found = []
    for m in _SKILL_TOKEN_RE.finditer(text):
        tok = m.group(1)
        # skip common short words
        if len(tok) <= 1:
            continue
        norm = _normalize_skill_token(tok)
        if norm and norm not in found:
            found.append(norm)
    # also try phrase matching for multi-word keys in _SKILL_CANON
    lower = text.lower()
    for phrase, canon in _SKILL_CANON.items():
        if " " in phrase and phrase in lower and canon not in found:
            found.append(canon)
    # fallback: keep up to top_n
    return found[:top_n]

def classify_company_group(company_name):
    if not company_name:
        return "Other"
    low = company_name.lower()
    for group, names in _COMPANY_GROUPS.items():
        # any substring match is acceptable (keeps it fuzzy)
        for n in names:
            if n in low:
                return group.title()  # Title-case label
    # heuristics: 'observability' in company name -> data observability
    if "observ" in low or "monitor" in low or "anomal" in low:
        return "Data Observability"
    if "catalog" in low or "govern" in low or "purview" in low:
        return "Data Intelligence"
    return "Other"

def detect_product_focus(text):
    """Return primary product focus label and a list of matched focus tokens (ordered)."""
    if not text:
        return ("Unknown", [])
    txt = text.lower()
    matches = []
    for label, kws in _PRODUCT_KEYWORDS.items():
        for kw in kws:
            if kw in txt:
                matches.append(label)
                break
    return (matches[0] if matches else "Other", matches)

def compute_relevancy_to_actian(row):
    """
    Explainable scoring:
      - skill hits for Actian (each hit adds)
      - product focus match (governance/etl/connector/streaming preferred)
      - geo match
      - seniority weighting
      - AI focus penalty or boost depending on Actian's strategy (this is tunable)
    returns float between 0..100 (rounded)
    """
    title = (row.get("Job Title") or "") + " " + (row.get("Job Link") or "") + " " + (row.get("Location") or "")
    skills = row.get("Extracted_Skills", [])
    product = row.get("Product_Focus","")
    senior = row.get("Seniority","Unknown")
    geo = (row.get("Location") or "").lower()

    score = 0.0
    # skill relevancy
    skill_hits = sum(1 for s in skills if s in _ACTIAN_RELEVANT_SKILLS)
    score += _WEIGHTS["skill_relevancy"] * skill_hits * 2.0

    # product relevancy
    prod_weight = 0.0
    if product in ("ETL/Integration","Data Governance","Data Observability","Streaming / Real-time"):
        prod_weight = 2.0
    score += _WEIGHTS["product_relevancy"] * prod_weight

    # geo
    geo_pref = 1.0 if any(g in geo for g in _ACTIAN_GEOS) else 0.0
    score += _WEIGHTS["geo_relevancy"] * geo_pref

    # seniority
    score += _WEIGHTS["seniority_relevancy"] * _SENIORITY_VALUE.get(senior, 0.1)

    # AI focus boost if job is AI/ML centric and Actian cares about ML/AI
    ai_tokens = {"AI","ML","MLOPS","MODEL","RAG","LLM"}
    ai_present = any(tok in (row.get("Extracted_Skills") or []) for tok in ai_tokens) or ("ai" in (title.lower()))
    score += _WEIGHTS["ai_focus"] * (1.0 if ai_present else 0.0)

    # normalize to 0..100
    raw = max(0.0, score)
    scaled = min(100.0, round(raw * 10.0, 1))  # tweak scaling factor to taste
    return scaled

def compute_ai_focus(row):
    title = (row.get("Job Title") or "").lower()
    desc = (row.get("Job Description") or "").lower()
    combined = title + " " + desc
    if re.search(r'\b(ai|llm|gpt|transformer|r[ea]g|retrieval-augmented|mlops|model monitoring|model serving|embedding)\b', combined, re.I):
        return True
    return False

def compute_connector_focus(row):
    # heuristics: connectors, replicat, ingestion, sources/destinations, connector names
    t = (row.get("Job Title") or "") + " " + (row.get("Job Description") or "")
    if re.search(r'\b(connector|connectors|replicate|replication|ingest|ingestion|source|destination|adapter|connector-sdk)\b', t, re.I):
        return True
    return False

def compute_trend_score(row):
    """
    Simple trend proxy:
      + postings count for this company (not available per row) would be best.
      Here we use proxies:
        - AI mention -> +2
        - Streaming mention -> +1.5
        - Observability mention -> +1.5
        - Senior / Staff roles suggest investment -> +1
    Returns 0..10
    """
    score = 0.0
    title_desc = ((row.get("Job Title") or "") + " " + (row.get("Job Description") or "")).lower()
    if re.search(r'\b(ai|ml|llm|gpt|r[ea]g|mlops)\b', title_desc):
        score += 2.0
    if re.search(r'\b(stream|kafka|realtime|real-time|pubsub|kinesis)\b', title_desc):
        score += 1.5
    if re.search(r'\b(observab|monitor|anomal)\b', title_desc):
        score += 1.5
    if row.get("Seniority","") in ("Senior","Senior/Lead","Principal/Staff","Director+"):
        score += 1.0
    # scale to 0..10
    return min(10.0, round(score,2))

# -------------------------
# main enrichment function
# -------------------------

def enrich_row(row):
    """
    Accepts a single row (dict). Mutates/augments with new fields:
      - Extracted_Skills (list)
      - Primary_Skill (first canonical)
      - Company_Group
      - Product_Focus
      - Product_Focus_Tokens (list)
      - Relevancy_to_Actian (0..100)
      - AI_Focus (bool)
      - Connector_Focus (bool)
      - Trend_Score (0..10)
    """
    # normalize input strings
    title = (row.get("Job Title") or "").strip()
    desc = (row.get("Job Description") or "")  # job description may be empty from scraper
    combined = " ".join([title, desc, row.get("Location","") or " "])

    # extract skills from title first, then desc
    skills_title = extract_skills_from_text(title)
    skills_desc = extract_skills_from_text(desc)
    # merge preserving order: title-priority then desc
    seen = set()
    skills = []
    for s in (skills_title + skills_desc):
        if s and s not in seen:
            seen.add(s); skills.append(s)

    # fallback: if no skills, try scanning link for tokens
    if not skills:
        link = (row.get("Job Link") or "").lower()
        for tok in ["gh_jid","greenhouse","lever","myworkday","bamboohr"]:
            if tok in link:
                # no skills to extract here, it's ok
                pass

    # primary skill
    primary = skills[0] if skills else ""

    # company group
    company_group = classify_company_group(row.get("Company",""))

    # product focus
    pfocus, pf_tokens = detect_product_focus(" ".join([title, desc]))

    # computed booleans
    ai_focus = compute_ai_focus({"Job Title": title, "Job Description": desc})
    connector_focus = compute_connector_focus({"Job Title": title, "Job Description": desc})

    # trend / relevancy
    # ensure Seniority exists (scraper already adds it). default Unknown if not.
    if "Seniority" not in row:
        row["Seniority"] = "Unknown"
    relevancy = compute_relevancy_to_actian({
        "Job Title": title,
        "Job Link": row.get("Job Link",""),
        "Location": row.get("Location",""),
        "Extracted_Skills": skills,
        "Product_Focus": pfocus,
        "Seniority": row.get("Seniority","Unknown"),
        "Job Description": desc
    })
    trend = compute_trend_score({
        "Job Title": title,
        "Job Description": desc,
        "Seniority": row.get("Seniority","Unknown")
    })

    # package fields
    row["Extracted_Skills"] = skills
    row["Primary_Skill"] = primary
    row["Company_Group"] = company_group
    row["Product_Focus"] = pfocus
    row["Product_Focus_Tokens"] = pf_tokens
    row["Relevancy_to_Actian"] = relevancy
    row["AI_Focus"] = ai_focus
    row["Connector_Focus"] = connector_focus
    row["Trend_Score"] = trend

    # return mutated row for chaining convenience
    return row

def enrich_rows(rows):
    """Enrich a list of rows and return the enriched list."""
    out = []
    for r in rows:
        try:
            out.append(enrich_row(r.copy()))
        except Exception as e:
            # keep original row if enrichment fails
            r["_enrich_error"] = str(e)
            out.append(r)
    return out

# -------------------------
# quick local test helper (not executed on import)
# -------------------------
if __name__ == "__main__":
    sample = {
        "Company": "Fivetran",
        "Job Title": "Senior Software Engineer - Connectors (Kafka, Snowflake)",
        "Job Link": "https://www.fivetran.com/careers/job?gh_jid=12345",
        "Location": "United States",
        "Posting Date": "2025-11-01",
        "Job Description": "Build connectors, stream data using Kafka, integrate with Snowflake and AWS. Must know Python and SQL."
    }
    print(enrich_row(sample))
