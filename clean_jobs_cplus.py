# clean_jobs_cplus.py  (ENTERPRISE C+ CLEANER — Category Upgrade vNext)

import pandas as pd
import re, json
from datetime import datetime, date
import os

# ---------- CONFIG ----------
SKILL_WORDS = [
    "python","sql","java","aws","azure","gcp","etl","spark","snowflake","dbt",
    "docker","kubernetes","airflow","ml","ai","tableau","lookml","hadoop","scala",
    "nosql","redshift","bigquery","hive","react","node","javascript","go","rust",
    "r"
]

def normalize_skill(token):
    if not isinstance(token, str):
        return ""
    token = re.sub(r"[\[\]\(\)\{\}]", "", token).strip()
    return token.upper()

# -----------------------------
# NEW: COMPETITOR CATEGORY MAP
# -----------------------------
CATEGORY_MAP = {
    # ---- Data Intelligence (metadata mgmt, governance, catalog) ----
    "Collibra": "Data Intelligence",
    "Informatica": "Data Intelligence",
    "Alex Solutions": "Data Intelligence",
    "Alation": "Data Intelligence",
    "Atlan": "Data Intelligence",
    "DataGalaxy": "Data Intelligence",
    "Pentaho": "Data Intelligence",
    "Microsoft": "Data Intelligence",    # Purview

    # ---- Data Observability ----
    "Monte Carlo": "Data Observability",
    "BigEye": "Data Observability",
    "Anomalo": "Data Observability",
    "Acceldata": "Data Observability",

    # ---- ETL / ELT / Data Integration Platforms ----
    "Fivetran": "Data Integration",
    "Matillion": "Data Integration",
    "Couchbase": "Data Integration",
    "Panoply": "Data Integration",
    "Boomi": "Data Integration",
    "SnapLogic": "Data Integration",
    "Talend": "Data Integration",

    # ---- Cloud Data Platforms / Lakehouse ----
    "Databricks": "Cloud Data Platform",
    "Snowflake": "Cloud Data Platform",
    "Cloudera": "Cloud Data Platform",
    "Exasol": "Cloud Data Platform",
    "Vertica": "Cloud Data Platform",
    "MariaDB": "Cloud Data Platform",

    # ---- Application Performance / Monitoring / Observability ----
    "Datadog": "Monitoring & APM",
    "New Relic": "Monitoring & APM",

    # ---- MDM / Governance ----
    "Precisely": "Master Data Management",
    "Syniti": "Master Data Management",

    # ---- Dev Infra / Cloud Infra ----
    "Nutanix": "Cloud Infrastructure",
    "Oracle": "Cloud Infrastructure",
    "Tencent": "Cloud Infrastructure",

    # ---- Other (productivity, misc) ----
    "Airtable": "Other",
    "Yellowbrick": "Other",
    "GoldenSource": "Other",
    "Qlik": "Other",
    "Data.World": "Other"
}

def detect_category(company):
    if not isinstance(company, str):
        return "Other"

    for key, cat in CATEGORY_MAP.items():
        if key.lower() in company.lower():
            return cat

    return "Other"


FUNCTION_KEYWORDS = {
    "Engineering": ["engineer","developer","sre","site reliability","platform","backend","frontend","devops","software","infrastructure"],
    "Data/Analytics": ["data engineer","data scientist","data analyst","analytics","machine learning","ml","etl","pipeline"],
    "Product": ["product manager","product","pm","ux","ux/ui","ui/ux","designer","ux"],
    "Sales": ["sales","account","business development","bd","account executive","ae"],
    "Customer Success": ["customer success","cs","support","implementation","onboarding"],
    "Marketing": ["marketing","growth","demand"],
    "Operations": ["ops","finance","hr","people","recruiter","legal"]
}

SENIORITY_PATTERNS = [
    ("Director+", r"\b(director|vp\b|vice president|head of|chief|c-|executive director|managing director)\b"),
    ("Senior/Lead", r"\b(senior|sr\.|sr\b|lead|principal|staff|architect|distinguished|fellow)\b"),
    ("Manager", r"\b(manager|mgr|people manager|engineering manager)\b"),
    ("Mid", r"\b(mid|intermediate|experienced|level ii|ii\b|iii|iv|2|3)\b"),
    ("Entry", r"\b(entry|junior|jr\.|graduate|associate|trainee)\b"),
    ("Intern", r"\b(intern|trainee|working student|werkstudent|internship)\b"),
]

COMPANY_FIXES = {
    "Fivetran": ["fivetran", "fivetran - launchers"],
    "Ataccama": ["ataccama","atacama"],
    "Datadog": ["datadog"],
}

SKIP_TITLE_PATTERNS = [
    r'\b(apply now|learn more|view openings|see all jobs|open positions|all jobs|careers home)\b',
    r'\b(learn more|read more|register|sign in|log in)\b'
]
SKIP_TITLE_RE = re.compile('|'.join(SKIP_TITLE_PATTERNS), re.I)

# ---------- HELPERS ----------
def clean_text(s):
    if not isinstance(s, str): 
        return ""
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'[^\x00-\x7F]+',' ', s)
    return s.strip(" -:,.")


def normalize_company(name):
    if not isinstance(name, str): return ""
    n = clean_text(name)
    n_low = n.lower()
    for canonical, variants in COMPANY_FIXES.items():
        for v in variants:
            if v in n_low:
                return canonical
    return n


def strip_location_from_title(title):
    t = title
    t = re.sub(r'\(([^)]+)\)\s*$', '', t).strip()
    t = re.sub(r'\s+[-|]\s*[A-Za-z0-9 ,\-()\/]+$', '', t).strip()

    parts = re.split(r'\s{2,}| - | \| | — | – |,', title)
    if len(parts) > 1:
        tail = parts[-1]
        if len(tail.split()) <= 4 and re.search(r'[A-Za-z]', tail):
            t = " ".join(parts[:-1]).strip()
    return t


def normalize_location_cell(loc):
    if not isinstance(loc, str) or not loc.strip(): return ""
    loc = re.sub(r'^(remote[\s\-:,]*)+', 'Remote, ', loc, flags=re.I)
    parts = [p.strip() for p in re.split(r'[,/;|]+', loc) if p.strip()]
    out, seen = [], set()
    for p in parts:
        k = p.lower()
        if k in seen: continue
        seen.add(k)
        if k == "remote": out.append("Remote")
        else: out.append(p.title())
    return ", ".join(out)


def extract_location_field(title, location_cell):
    if isinstance(location_cell, str) and location_cell.strip():
        return normalize_location_cell(location_cell)

    m = re.search(r'\(([^)]+)\)\s*$', title)
    if m:
        return normalize_location_cell(m.group(1))

    parts = re.split(r'\s+-\s+|\s+\|\s+|\s+—\s+', title)
    if len(parts) > 1:
        cand = parts[-1]
        if len(cand.split()) <= 4 and re.search(r'[A-Za-z]', cand):
            return normalize_location_cell(cand)
    return ""


def classify_function(title):
    tl = (title or "").lower()
    for func, kws in FUNCTION_KEYWORDS.items():
        for k in kws:
            if k in tl:
                return func
    return "Other"


def classify_seniority(title, location=""):
    if not title:
        return "Unknown"
    t = (title + " " + (location or "")).lower()

    if any(x in t for x in ["chief ", "cxo ", "cto", "ceo", "cfo", "coo", "vp ", "vice president", "svps", "evp", "executive director", "head of", "director", "managing director"]):
        return "Director+"
    if any(x in t for x in ["principal", "distinguished", "fellow", "lead", "staff", "senior", "sr."]):
        return "Senior/Lead"
    if any(x in t for x in ["manager", "people manager", "engineering manager", "mgr"]):
        return "Manager"
    if any(x in t for x in ["mid ", "intermediate", "level ii", "ii ", "2 "]):
        return "Mid"
    if any(x in t for x in ["junior", "jr.", "jr ", "entry", "graduate", "associate", "trainee"]):
        return "Entry"
    if any(x in t for x in ["intern", "internship", "working student", "werkstudent"]):
        return "Intern"
    if re.search(r'\b(ii|iii|iv|2|3)\b', t):
        return "Mid"
    return "Unknown"


def extract_skills(title):
    tl = (title or "").lower()
    extracted = set()

    for skill in SKILL_WORDS:
        if skill in tl:
            extracted.add(normalize_skill(skill))

    bracketed = re.findall(r"[\[\(\{]([a-zA-Z0-9\+]+)[\]\)\}]", title)
    for b in bracketed:
        extracted.add(normalize_skill(b))

    return list(extracted)

# ---------- MAIN ----------
def main():
    repo_root = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(repo_root, "jobs_final_hard.csv")
    
    if not os.path.exists(input_path):
        print(f"[ERROR] Input file not found: {input_path}")
        return

    df = pd.read_csv(input_path, dtype=str).fillna("")

    df["Company"] = df["Company"].apply(normalize_company)
    df["Job Title"] = df["Job Title"].apply(clean_text)
    df["Job Link"] = df["Job Link"].str.strip()
    df = df[~df["Job Title"].str.match(SKIP_TITLE_RE)]

    def best_row(group):
        with_date = group[group["Posting Date"].str.len() > 0]
        if len(with_date) > 0:
            return with_date.iloc[0]
        return group.iloc[0]

    df = df.groupby("Job Link", as_index=False).apply(best_row).reset_index(drop=True)

    df["Location"] = df.apply(lambda r: extract_location_field(r["Job Title"], r["Location"]), axis=1)
    df["Location"] = df["Location"].apply(normalize_location_cell)

    df["Job Title"] = df["Job Title"].apply(strip_location_from_title)
    df["Job Title"] = df["Job Title"].apply(lambda t: re.sub(r'\s{2,}', ' ', t).strip(" -,."))
    
    df["Function"] = df["Job Title"].apply(classify_function)
    df["Seniority"] = df.apply(lambda r: classify_seniority(r["Job Title"], r["Location"]), axis=1)
    df["Skills_in_Title"] = df["Job Title"].apply(extract_skills)

    df["Category"] = df["Company"].apply(detect_category)

    def compute_days(pd_str):
        if not pd_str: return ""
        try:
            d_part = pd_str.split('T')[0]
            d = datetime.fromisoformat(d_part).date()
            return (date.today() - d).days
        except:
            try:
                d_part = pd_str.split(' ')[0]
                d = datetime.strptime(d_part, "%Y-%m-%d").date()
                return (date.today() - d).days
            except:
                return ""

    df["Days Since Posted"] = df["Posting Date"].apply(compute_days)

    df = df.fillna("")
    df = df.sort_values(by=["Company","Job Title"])
    df["Skills_in_Title"] = df["Skills_in_Title"].apply(lambda x: json.dumps(x))

    out_cols = [
        "Company","Job Title","Job Link","Location",
        "Posting Date","Days Since Posted",
        "Function","Seniority","Skills_in_Title","Category"
    ]

    out_path = os.path.join(repo_root, "jobs_cleaned_final_enriched.csv")
    df[out_cols].to_csv(out_path, index=False)

    print("[OK] wrote jobs_cleaned_final_enriched.csv with", len(df), "rows")

if __name__ == "__main__":
    main()
