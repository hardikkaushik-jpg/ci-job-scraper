# clean_jobs_cplus.py
# Cleaner / Enricher 4.0 (C2 Variant: No Job Description in final CSV)
# - Keeps enrichment + scoring logic intact
# - Does NOT output "Job Description" column
# - Internal description is still allowed (if scraper provides it)

import re
import json
import csv
from datetime import date
from typing import List, Dict
from collections import defaultdict

# ----------------------------------------------------
# Canonical skill mapping
# ----------------------------------------------------
_SKILL_CANON = {
    "python": "PYTHON", "python3": "PYTHON", "py": "PYTHON",
    "r": "R",
    "sql": "SQL", "tsql": "SQL",
    "postgres": "POSTGRES", "postgresql": "POSTGRES",
    "aws": "AWS", "amazon web services": "AWS",
    "gcp": "GCP", "google cloud": "GCP",
    "azure": "AZURE",
    "spark": "SPARK", "kafka": "KAFKA",
    "dbt": "DBT", "airflow": "AIRFLOW",
    "etl": "ETL", "elt": "ELT",
    "java": "JAVA", "scala": "SCALA",
    "go": "GO", "golang": "GO",
    "rust": "RUST",
    "javascript": "JAVASCRIPT", "js": "JAVASCRIPT",
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
    "lineage": "LINEAGE",
    "metadata": "METADATA",
    "ml": "ML", "machine learning": "ML",
    "ai": "AI",
    "streaming": "STREAMING", "spark streaming": "STREAMING",
    "prometheus": "PROMETHEUS",
    "grafana": "GRAFANA"
}

_SKILL_TOKEN_RE = re.compile(r'\b([A-Za-z+#\.\-/]{1,40})\b')

# ----------------------------------------------------
# Company groups
# ----------------------------------------------------
_COMPANY_GROUPS = {
    "data intelligence": {"collibra","informatica","atlan","alation","datagalaxy","pentaho","microsoft purview"},
    "data observability": {"acceldata","anomalo","bigeye","monte carlo"},
    "etl/connectors": {"fivetran","airbyte","talend","matillion","snaplogic","boomi"},
    "warehouse/processing": {"snowflake","databricks","redshift","bigquery","teradata","vertica"},
    "monitoring/platforms": {"datadog","splunk","new relic"},
}

# ----------------------------------------------------
# Product focus keywords
# ----------------------------------------------------
_PRODUCT_KEYWORDS = {
    "Data Quality": ["data quality","data health","accuracy","data testing"],
    "Data Observability": ["observability","monitor","anomaly","alert","telemetry"],
    "Data Governance": ["governance","catalog","glossary","lineage","metadata"],
    "ETL/Integration": ["etl","integrat","replicat","pipeline","sync","connector"],
    "Streaming / Real-time": ["stream","kafka","real-time","pubsub","kinesis"],
    "ML/AI infra": ["ml","machine learning","mlops","model","ai"],
    "Platform / Infra": ["sre","infra","platform","kubernetes","aws","gcp","azure"]
}

# ----------------------------------------------------
# Actian relevancy scoring config
# ----------------------------------------------------
_ACTIAN_RELEVANT_SKILLS = {"ETL","DBT","SQL","AWS","AZURE","GCP","SNOWFLAKE","DATABRICKS","POSTGRES","STREAMING","KAFKA","RUST","GO"}

_WEIGHTS = {
    "skill_relevancy": 1.0,
    "product_relevancy": 1.5,
    "geo_relevancy": 0.6,
    "seniority_relevancy": 0.3,
    "ai_focus": 0.8
}

_ACTIAN_GEOS = {"united states","germany","india","uk","singapore","canada","australia"}

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

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def _normalize_skill_token(tok: str):
    if not tok:
        return None
    low = tok.lower()
    low_clean = low.strip(".+-() ")
    if low_clean in _SKILL_CANON:
        return _SKILL_CANON[low_clean]
    if low in ("r",):
        return "R"
    if re.match(r'^py(thon)?[0-9]*$', low):
        return "PYTHON"
    if len(low_clean) <= 5 and low_clean.isalpha():
        return low_clean.upper()
    return None

def extract_skills_from_text(text, top_n=12):
    if not text:
        return []
    found = []
    lower = text.lower()

    for m in _SKILL_TOKEN_RE.finditer(text):
        norm = _normalize_skill_token(m.group(1))
        if norm and norm not in found:
            found.append(norm)

    for phrase, canon in _SKILL_CANON.items():
        if " " in phrase and phrase in lower and canon not in found:
            found.append(canon)

    return found[:top_n]

def classify_company_group(company):
    if not company:
        return "Other"
    low = company.lower()
    for group, names in _COMPANY_GROUPS.items():
        if any(n in low for n in names):
            return group.title()
    return "Other"

def detect_product_focus(text):
    if not text:
        return ("Other", [])
    txt = text.lower()
    matches = []
    for label, kws in _PRODUCT_KEYWORDS.items():
        if any(kw in txt for kw in kws):
            matches.append(label)
    return (matches[0] if matches else "Other", matches)

def compute_relevancy_to_actian(row):
    skills = row["Extracted_Skills"]
    score = 0.0

    score += _WEIGHTS["skill_relevancy"] * (2 * sum(1 for s in skills if s in _ACTIAN_RELEVANT_SKILLS))

    if row["Product_Focus"] in ("ETL/Integration","Data Governance","Data Observability","Streaming / Real-time"):
        score += _WEIGHTS["product_relevancy"] * 2.0

    if any(g in row["Location"].lower() for g in _ACTIAN_GEOS):
        score += _WEIGHTS["geo_relevancy"]

    score += _WEIGHTS["seniority_relevancy"] * _SENIORITY_VALUE.get(row["Seniority"], 0.1)

    if any(x in skills for x in ("AI","ML","MLOPS","MODEL")):
        score += _WEIGHTS["ai_focus"]

    return min(100.0, round(max(0, score) * 10, 1))

def compute_trend_score(row):
    text = (row["Job Title"] + " " + row["Job Description"]).lower()
    s = 0.0
    if re.search(r"ai|ml|llm|gpt|rag|mlops", text): s += 2
    if re.search(r"stream|kafka|real-time", text): s += 1.5
    if re.search(r"observab|monitor|anomal", text): s += 1.5
    if row["Seniority"] in ("Senior","Principal/Staff","Director+"): s += 1.0
    return min(10.0, round(s, 2))

def infer_function_from_title(t):
    if not t: return ""
    t = t.lower()
    if re.search(r"engineer|developer|devops|sre|software", t): return "Engineering"
    if "data" in t: return "Data/Analytics"
    if "product" in t: return "Product"
    if "sales" in t: return "Sales"
    if "marketing" in t: return "Marketing"
    if "hr" in t or "talent" in t: return "People/HR"
    if "ops" in t or "support" in t: return "Operations"
    return "Other"

# ----------------------------------------------------
# Row Enrichment
# ----------------------------------------------------
def enrich_row(row):
    title = row["Job Title"]
    desc = row.get("Job Description","")
    combined = title + " " + desc + " " + row.get("Location","")

    # Skills
    skills_title = extract_skills_from_text(title)
    skills_desc = extract_skills_from_text(desc)
    final_skills = list(dict.fromkeys(skills_title + skills_desc))

    # Company group + product focus
    company_group = classify_company_group(row["Company"])
    pfocus, pf_tokens = detect_product_focus(combined)

    # Relevancy scoring
    rel = compute_relevancy_to_actian({
        "Job Title": title,
        "Location": row["Location"],
        "Extracted_Skills": final_skills,
        "Product_Focus": pfocus,
        "Seniority": row.get("Seniority","Unknown"),
        "Job Description": desc
    })

    trend = compute_trend_score({
        "Job Title": title,
        "Job Description": desc,
        "Seniority": row.get("Seniority","Unknown")
    })

    # Add fields
    row["Extracted_Skills"] = final_skills
    row["Primary_Skill"] = final_skills[0] if final_skills else ""
    row["Company_Group"] = company_group
    row["Product_Focus"] = pfocus
    row["Product_Focus_Tokens"] = pf_tokens
    row["Relevancy_to_Actian"] = rel
    row["Trend_Score"] = trend
    row["Skills_in_Title"] = ",".join(skills_title)
    row["Function"] = infer_function_from_title(title)

    return row

def enrich_rows(rows):
    out = []
    for r in rows:
        try:
            out.append(enrich_row(r))
        except Exception as e:
            r["_enrich_error"] = str(e)
            out.append(r)
    return out

# ----------------------------------------------------
# CLI Runner
# ----------------------------------------------------
def main():
    import os
    repo_root = os.path.dirname(os.path.abspath(__file__))
    infile = os.path.join(repo_root, "jobs_final_hard.csv")
    outfile = os.path.join(repo_root, "jobs_cleaned_final_enriched.csv")

    # Load raw
    rows = []
    with open(infile, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "Company": r.get("Company",""),
                "Job Title": r.get("Job Title",""),
                "Job Link": r.get("Job Link",""),
                "Location": r.get("Location",""),
                "Posting Date": r.get("Posting Date",""),
                "Days Since Posted": r.get("Days Since Posted",""),
                "Seniority": r.get("Seniority","Unknown"),
                "Job Description": r.get("Job Description","") or ""
            })

    print(f"[CLEANER] Loaded {len(rows)} rows. Enriching...")
    enriched = enrich_rows(rows)

    # Serialize lists to JSON
    for r in enriched:
        r["Extracted_Skills"] = json.dumps(r["Extracted_Skills"])
        r["Product_Focus_Tokens"] = json.dumps(r["Product_Focus_Tokens"])

    # FINAL OUTPUT COLUMNS — NO DESCRIPTION
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
            out = {col: r.get(col,"") for col in fieldnames}
            w.writerow(out)

    print(f"[CLEANER] Wrote enriched file: {outfile} ({len(enriched)} rows)")

if __name__ == "__main__":
    main()
