# clean_jobs_cplus.py
# Cleaner / Enricher 4.0 C3 (NO DESCRIPTION VERSION)
# - Receives ONLY the scraper output (7 fields)
# - Performs full enrichment without job description
# - 100% compatible with jobs_smart_cplus.py, validator, workflow

import re
import json
import csv
import os

# ----------------------------------------------------
# Canonical skill mapping
# ----------------------------------------------------
_SKILL_CANON = {
    "python": "PYTHON", "python3": "PYTHON", "py": "PYTHON",
    "r": "R",
    "sql": "SQL", "tsql": "SQL",
    "postgres": "POSTGRES", "postgresql": "POSTGRES",
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "AZURE",
    "spark": "SPARK",
    "kafka": "KAFKA",
    "dbt": "DBT",
    "airflow": "AIRFLOW",
    "etl": "ETL", "elt": "ELT",
    "java": "JAVA", "scala": "SCALA",
    "go": "GO",
    "rust": "RUST",
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
    "ml": "ML",
    "ai": "AI",
    "streaming": "STREAMING",
    "prometheus": "PROMETHEUS",
    "grafana": "GRAFANA"
}

_SKILL_TOKEN_RE = re.compile(r'\b([A-Za-z0-9+#\.\-/]{1,40})\b')

# ----------------------------------------------------
# Company groups
# ----------------------------------------------------
_COMPANY_GROUPS = {
    "data intelligence": 
        {"collibra","informatica","atlan","alation","datagalaxy","pentaho"},
    "data observability": 
        {"acceldata","anomalo","bigeye","monte carlo"},
    "etl/connectors": 
        {"fivetran","airbyte","talend","matillion","snaplogic","boomi"},
    "warehouse/processing": 
        {"snowflake","databricks","redshift","bigquery","teradata","vertica"},
    "monitoring/platforms": 
        {"datadog","splunk","new relic"},
}

# ----------------------------------------------------
# Product focus keywords
# ----------------------------------------------------
_PRODUCT_KEYWORDS = {
    "Data Quality": ["data quality","accuracy","data testing"],
    "Data Observability": ["observability","monitor","anomaly","alert","telemetry"],
    "Data Governance": ["governance","catalog","glossary","lineage","metadata"],
    "ETL/Integration": ["etl","integrat","replicat","pipeline","sync","connector"],
    "Streaming / Real-time": ["stream","kafka","real-time"],
    "ML/AI infra": ["ml","machine learning","mlops","model","ai"],
    "Platform / Infra": ["sre","infra","platform","kubernetes","aws","gcp","azure"]
}

# ----------------------------------------------------
# Relevancy-to-Actian scoring config
# ----------------------------------------------------
_ACTIAN_RELEVANT_SKILLS = {
    "ETL","DBT","SQL","AWS","AZURE","GCP",
    "SNOWFLAKE","DATABRICKS","POSTGRES","STREAMING","KAFKA","RUST","GO"
}

_WEIGHTS = {
    "skill_relevancy": 1.0,
    "product_relevancy": 1.5,
    "geo_relevancy": 0.6,
    "seniority_relevancy": 0.3,
    "ai_focus": 0.8
}

_ACTIAN_GEOS = {
    "united states","germany","india","uk","singapore","canada","australia"
}

_SENIORITY_VALUE = {
    "Director+": 1.0, "Principal/Staff": 0.9, "Senior": 0.7,
    "Manager": 0.6, "Mid": 0.4, "Entry": 0.2, "Intern": 0.05,
    "Unknown": 0.1
}

# ----------------------------------------------------
# Skill extraction
# ----------------------------------------------------
def _normalize_skill_token(tok):
    if not tok: return None
    t = tok.lower().strip(".+-() ")
    return _SKILL_CANON.get(t)

def extract_skills(text):
    if not text: return []
    skills = []
    low = text.lower()

    for m in _SKILL_TOKEN_RE.finditer(text):
        norm = _normalize_skill_token(m.group(1))
        if norm and norm not in skills:
            skills.append(norm)

    # multi-word patterns
    for phrase, canon in _SKILL_CANON.items():
        if " " in phrase and phrase in low and canon not in skills:
            skills.append(canon)

    return skills[:12]

# ----------------------------------------------------
# Company group classification
# ----------------------------------------------------
def classify_company_group(company):
    low = company.lower()
    for group, words in _COMPANY_GROUPS.items():
        if any(w in low for w in words):
            return group.title()
    return "Other"

# ----------------------------------------------------
# Product focus detection
# ----------------------------------------------------
def detect_product_focus(text):
    if not text: return ("Other", [])
    txt = text.lower()
    matches = [label for label, kws in _PRODUCT_KEYWORDS.items() if any(kw in txt for kw in kws)]
    return (matches[0] if matches else "Other", matches)

# ----------------------------------------------------
# Relevancy scoring
# ----------------------------------------------------
def compute_relevancy_to_actian(title, location, skills, pfocus, seniority):
    score = 0.0
    score += _WEIGHTS["skill_relevancy"] * (2 * sum(1 for s in skills if s in _ACTIAN_RELEVANT_SKILLS))

    if pfocus in ("ETL/Integration","Data Governance","Data Observability","Streaming / Real-time"):
        score += 2 * _WEIGHTS["product_relevancy"]

    if any(g in location.lower() for g in _ACTIAN_GEOS):
        score += _WEIGHTS["geo_relevancy"]

    score += _WEIGHTS["seniority_relevancy"] * _SENIORITY_VALUE.get(seniority, 0.1)

    if any(x in skills for x in ("AI","ML","MLOPS","MODEL")):
        score += _WEIGHTS["ai_focus"]

    return min(100.0, round(max(0, score) * 10, 1))

# ----------------------------------------------------
# Trend score (title-only)
# ----------------------------------------------------
def compute_trend_score(title, seniority):
    text = title.lower()
    s = 0
    if re.search(r"ai|ml|llm|gpt|rag|mlops", text): s += 2
    if re.search(r"stream|kafka|real-time", text): s += 1.5
    if re.search(r"observab|monitor|anomal", text): s += 1.5
    if seniority in ("Senior","Principal/Staff","Director+"): s += 1.0
    return min(10.0, round(s, 2))

# ----------------------------------------------------
# Function inference
# ----------------------------------------------------
def infer_function(t):
    t = t.lower()
    if re.search(r"(engineer|developer|devops|sre|software)", t): return "Engineering"
    if "data" in t: return "Data/Analytics"
    if "product" in t: return "Product"
    if "sales" in t: return "Sales"
    if "marketing" in t: return "Marketing"
    if "hr" in t or "talent" in t: return "People/HR"
    if "ops" in t or "support" in t: return "Operations"
    return "Other"

# ----------------------------------------------------
# Enrich a single row
# ----------------------------------------------------
def enrich_row(r):
    title = r["Job Title"]
    location = r["Location"]
    seniority = r["Seniority"]

    combined = title + " " + location

    skills_title = extract_skills(title)
    extracted_skills = list(dict.fromkeys(skills_title))

    company_group = classify_company_group(r["Company"])
    pfocus, pf_tokens = detect_product_focus(combined)

    relevancy = compute_relevancy_to_actian(title, location, extracted_skills, pfocus, seniority)
    trend = compute_trend_score(title, seniority)

    r["Extracted_Skills"] = extracted_skills
    r["Primary_Skill"] = extracted_skills[0] if extracted_skills else ""
    r["Company_Group"] = company_group
    r["Product_Focus"] = pfocus
    r["Product_Focus_Tokens"] = pf_tokens
    r["Relevancy_to_Actian"] = relevancy
    r["Trend_Score"] = trend
    r["Skills_in_Title"] = ",".join(skills_title)
    r["Function"] = infer_function(title)

    return r

# ----------------------------------------------------
# Main
# ----------------------------------------------------
def main():
    repo = os.path.dirname(os.path.abspath(__file__))
    infile = os.path.join(repo, "jobs_final_hard.csv")
    outfile = os.path.join(repo, "jobs_cleaned_final_enriched.csv")

    rows = []
    with open(infile, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "Company": r["Company"],
                "Job Title": r["Job Title"],
                "Job Link": r["Job Link"],
                "Location": r["Location"],
                "Posting Date": r["Posting Date"],
                "Days Since Posted": r["Days Since Posted"],
                "Seniority": r["Seniority"]
            })

    enriched = [enrich_row(r) for r in rows]

    # serialize lists → JSON
    for r in enriched:
        r["Extracted_Skills"] = json.dumps(r["Extracted_Skills"])
        r["Product_Focus_Tokens"] = json.dumps(r["Product_Focus_Tokens"])

    fieldnames = [
        "Company","Job Title","Job Link","Location","Posting Date","Days Since Posted",
        "Function","Seniority","Skills_in_Title",
        "Company_Group","Product_Focus","Product_Focus_Tokens","Primary_Skill",
        "Extracted_Skills","Relevancy_to_Actian","Trend_Score"
    ]

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in enriched:
            w.writerow({c: r.get(c,"") for c in fieldnames})

    print(f"[CLEANER] Wrote enriched file: {outfile} ({len(enriched)} rows)")

if __name__ == "__main__":
    main()
