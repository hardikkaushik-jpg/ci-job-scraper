# clean_jobs_cplus.py
# Cleaner / Enricher v3 for jobs pipeline (canonical)
# Usage:
#   - import enrich_rows(rows) from this module (used by the scraper)
#   - run as CLI: python clean_jobs_cplus.py  (reads jobs_final_hard.csv, writes jobs_cleaned_final_enriched.csv)

import re
import json
import csv
from datetime import date
from collections import defaultdict
from typing import List, Dict

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
}

# tokenization regex for skills detection (word boundaries + common characters)
_SKILL_TOKEN_RE = re.compile(r'\b([A-Za-z+#\.\-/]{1,40})\b')

# Competitor grouping
_COMPANY_GROUPS = {
    "data intelligence": {"collibra","informatica","atlan","alation","datagalaxy","pentaho","microsoft purview","alex solutions"},
    "data observability": {"acceldata","anomalo","bigeye","monte carlo"},
    "etl/connectors": {"fivetran","airbyte","talend","matillion","snaplogic","boomi"},
    "warehouse/processing": {"snowflake","databricks","redshift","bigquery","teradata","vertica"},
    "monitoring/platforms": {"datadog","splunk","new relic"},
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

# Actian-relevant skills
_ACTIAN_RELEVANT_SKILLS = {"ETL","DBT","SQL","AWS","AZURE","GCP","SNOWFLAKE","DATABRICKS","POSTGRES","STREAMING","KAFKA","RUST","GO"}

# score weights (tweakable)
_WEIGHTS = {
    "skill_relevancy": 1.0,
    "product_relevancy": 1.5,
    "geo_relevancy": 0.6,
    "seniority_relevancy": 0.3,
    "ai_focus": 0.8
}

# geo preferences for Actian
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

# -------------------------
# helper functions
# -------------------------
def _normalize_skill_token(tok: str):
    if not tok:
        return None
    low = tok.strip().lower()
    low = low.replace("()", "")
    if low in _SKILL_CANON:
        return _SKILL_CANON[low]
    low_clean = low.strip(".+-")
    if low_clean in _SKILL_CANON:
        return _SKILL_CANON[low_clean]
    if re.match(r'^(py(thon)?)(\d)?$', low):
        return "PYTHON"
    if low in ("r",):
        return "R"
    if low in ("sql", "tsql"):
        return "SQL"
    if len(low) <= 5 and low.isalpha():
        return low.upper()
    return None

def extract_skills_from_text(text, top_n=12):
    if not text:
        return []
    found = []
    for m in _SKILL_TOKEN_RE.finditer(text):
        tok = m.group(1)
        if len(tok) <= 1:
            continue
        norm = _normalize_skill_token(tok)
        if norm and norm not in found:
            found.append(norm)
    lower = text.lower()
    for phrase, canon in _SKILL_CANON.items():
        if " " in phrase and phrase in lower and canon not in found:
            found.append(canon)
    return found[:top_n]

def classify_company_group(company_name):
    if not company_name:
        return "Other"
    low = company_name.lower()
    for group, names in _COMPANY_GROUPS.items():
        for n in names:
            if n in low:
                return group.title()
    if "observ" in low or "monitor" in low or "anomal" in low:
        return "Data Observability"
    if "catalog" in low or "govern" in low or "purview" in low:
        return "Data Intelligence"
    return "Other"

def detect_product_focus(text):
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
    title = (row.get("Job Title") or "") + " " + (row.get("Job Link") or "") + " " + (row.get("Location") or "")
    skills = row.get("Extracted_Skills", [])
    product = row.get("Product_Focus","")
    senior = row.get("Seniority","Unknown")
    geo = (row.get("Location") or "").lower()

    score = 0.0
    skill_hits = sum(1 for s in skills if s in _ACTIAN_RELEVANT_SKILLS)
    score += _WEIGHTS["skill_relevancy"] * skill_hits * 2.0

    prod_weight = 0.0
    if product in ("ETL/Integration","Data Governance","Data Observability","Streaming / Real-time"):
        prod_weight = 2.0
    score += _WEIGHTS["product_relevancy"] * prod_weight

    geo_pref = 1.0 if any(g in geo for g in _ACTIAN_GEOS) else 0.0
    score += _WEIGHTS["geo_relevancy"] * geo_pref

    score += _WEIGHTS["seniority_relevancy"] * _SENIORITY_VALUE.get(senior, 0.1)

    ai_tokens = {"AI","ML","MLOPS","MODEL","RAG","LLM"}
    ai_present = any(tok in (row.get("Extracted_Skills") or []) for tok in ai_tokens) or ("ai" in (title.lower()))
    score += _WEIGHTS["ai_focus"] * (1.0 if ai_present else 0.0)

    raw = max(0.0, score)
    scaled = min(100.0, round(raw * 10.0, 1))
    return scaled

def compute_ai_focus_row(title, desc):
    combined = (title or "") + " " + (desc or "")
    if re.search(r'\b(ai|llm|gpt|transformer|r[ea]g|retrieval-augmented|mlops|model monitoring|model serving|embedding)\b', combined, re.I):
        return True
    return False

def compute_connector_focus_row(title, desc):
    combined = (title or "") + " " + (desc or "")
    if re.search(r'\b(connector|connectors|replicate|replication|ingest|ingestion|source|destination|adapter|connector-sdk)\b', combined, re.I):
        return True
    return False

def compute_trend_score(row):
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
    return min(10.0, round(score,2))

# small helper to infer function (Function column required by validator)
def infer_function_from_title(title: str) -> str:
    if not title:
        return ""
    t = title.lower()
    if re.search(r'\b(engineer|developer|devops|sre|site reliability|software)\b', t):
        return "Engineering"
    if re.search(r'\b(data engineer|data platform|etl|etl engineer|pipeline)\b', t):
        return "Engineering"
    if re.search(r'\b(data scientist|ml|machine learning|mlops)\b', t):
        return "Data Science"
    if re.search(r'\b(product manager|product)\b', t):
        return "Product"
    if re.search(r'\b(sales|account executive|account manager)\b', t):
        return "Sales"
    if re.search(r'\b(marketing|demand|growth)\b', t):
        return "Marketing"
    if re.search(r'\b(hr|people|talent)\b', t):
        return "People/HR"
    if re.search(r'\b(operat|ops|support|customer)\b', t):
        return "Operations"
    if re.search(r'\b(design|ux|ui)\b', t):
        return "Design"
    if re.search(r'\b(consultant|professional services)\b', t):
        return "Professional Services"
    return "Other"

# -------------------------
# enrichment functions
# -------------------------
def enrich_row(row: Dict) -> Dict:
    title = (row.get("Job Title") or "").strip()
    desc = (row.get("Job Description") or "")
    combined = " ".join([title, desc, row.get("Location","") or " "])

    skills_title = extract_skills_from_text(title)
    skills_desc = extract_skills_from_text(desc)
    seen = set()
    skills = []
    for s in (skills_title + skills_desc):
        if s and s not in seen:
            seen.add(s); skills.append(s)

    primary = skills[0] if skills else ""

    company_group = classify_company_group(row.get("Company",""))

    pfocus, pf_tokens = detect_product_focus(" ".join([title, desc]))

    ai_focus = compute_ai_focus_row(title, desc)
    connector_focus = compute_connector_focus_row(title, desc)

    if "Seniority" not in row:
        # fallback; keep existing if present
        row["Seniority"] = row.get("Seniority") or "Unknown"

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

    # Package
    row["Extracted_Skills"] = skills
    row["Primary_Skill"] = primary
    row["Company_Group"] = company_group
    row["Product_Focus"] = pfocus
    row["Product_Focus_Tokens"] = pf_tokens
    row["Relevancy_to_Actian"] = relevancy
    row["AI_Focus"] = ai_focus
    row["Connector_Focus"] = connector_focus
    row["Trend_Score"] = trend

    # Extra small computed fields for validator & downstream
    row["Function"] = infer_function_from_title(title)
    # Skills_in_Title: comma-separated canonical tokens found in title (not description)
    row["Skills_in_Title"] = ",".join(skills_title) if skills_title else ""
    return row

def enrich_rows(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        try:
            out.append(enrich_row(r.copy()))
        except Exception as e:
            r["_enrich_error"] = str(e)
            out.append(r)
    return out

# -------------------------
# CLI: read raw CSV -> run enrich_rows -> write enriched CSV
# -------------------------
def main():
    import os
    repo_root = os.path.dirname(os.path.abspath(__file__))
    infile = os.path.join(repo_root, "jobs_final_hard.csv")
    outfile = os.path.join(repo_root, "jobs_cleaned_final_enriched.csv")

    # read input
    rows = []
    try:
        with open(infile, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                # ensure keys exist
                rows.append({
                    "Company": r.get("Company",""),
                    "Job Title": r.get("Job Title",""),
                    "Job Link": r.get("Job Link",""),
                    "Location": r.get("Location",""),
                    "Posting Date": r.get("Posting Date",""),
                    "Days Since Posted": r.get("Days Since Posted",""),
                    "Seniority": r.get("Seniority",""),
                    "Job Description": r.get("Job Description","") or ""
                })
    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {infile}")
        return
    except Exception as e:
        print(f"[ERROR] Could not read input file: {e}")
        return

    print(f"[CLEANER] Read {len(rows)} raw rows. Running enrichment...")
    enriched = enrich_rows(rows)

    # serialize lists before writing
    for r in enriched:
        r["Extracted_Skills"] = json.dumps(r.get("Extracted_Skills", []), ensure_ascii=False)
        r["Product_Focus_Tokens"] = json.dumps(r.get("Product_Focus_Tokens", []), ensure_ascii=False)
        r["AI_Focus"] = str(r.get("AI_Focus", False))
        r["Connector_Focus"] = str(r.get("Connector_Focus", False))

    # Define field order - validator expects certain columns
    fieldnames = [
        "Company","Job Title","Job Link","Location","Posting Date","Days Since Posted",
        "Function","Seniority","Skills_in_Title",
        "Company_Group","Product_Focus","Product_Focus_Tokens","Primary_Skill","Extracted_Skills",
        "Relevancy_to_Actian","AI_Focus","Connector_Focus","Trend_Score","Job Description"
    ]

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in enriched:
            row_to_write = {k: (r.get(k,"") if r.get(k) is not None else "") for k in fieldnames}
            writer.writerow(row_to_write)

    print(f"[CLEANER] Wrote enriched file: {outfile} ({len(enriched)} rows)")

if __name__ == "__main__":
    main()
