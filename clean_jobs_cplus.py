# clean_jobs_cplus.py  (ENTERPRISE C+ CLEANER — FINAL)
import pandas as pd
import re, json
from datetime import datetime, date

# ---------- CONFIG ----------
SKILL_WORDS = [
    "python","sql","java","aws","azure","gcp","etl","spark","snowflake","dbt",
    "docker","kubernetes","airflow","ml","ai","tableau","lookml","hadoop","scala",
    "nosql","redshift","bigquery","hive","react","node","javascript","go","rust"
]

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
    ("Director+", r"\b(director|vp\b|vice president|head of|chief|c-)\b"),
    ("Senior/Lead", r"\b(senior|sr\.|sr\b|lead|principal|staff|architect)\b"),
    ("Mid", r"\b(mid|intermediate|experienced|level ii|ii\b)\b"),
    ("Entry", r"\b(entry|junior|jr\.|graduate)\b"),
    ("Intern", r"\b(intern|trainee|working student|werkstudent)\b"),
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
    if not isinstance(s, str): return ""
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

    # remove "(London)" etc
    t = re.sub(r'\(([^)]+)\)\s*$', '', t).strip()

    # remove " - Berlin"
    t = re.sub(r'\s+[-|]\s*[A-Za-z0-9 ,\-()\/]+$', '', t).strip()

    # remove tail ", Berlin"
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
    t = (title or "") + " " + (location or "")
    for label, pattern in SENIORITY_PATTERNS:
        if re.search(pattern, t, re.I):
            return label
    return "Unknown"


def extract_skills(title):
    tl = (title or "").lower()
    return list({s for s in SKILL_WORDS if s in tl})



# ---------- MAIN ----------
def main():
    df = pd.read_csv("jobs_final_hard.csv", dtype=str).fillna("")

    # -------------------------
    # BASIC NORMALIZATION
    # -------------------------
    df["Company"] = df["Company"].apply(normalize_company)
    df["Job Title"] = df["Job Title"].apply(clean_text)
    df["Job Link"] = df["Job Link"].str.strip()

    # skip garbage titles
    df = df[~df["Job Title"].str.match(SKIP_TITLE_RE)]


    # -------------------------
    # FIX DUPLICates — keep row WITH posting date
    # -------------------------
    def best_row(group):
        with_date = group[group["Posting Date"].str.len() > 0]
        if len(with_date) > 0:
            return with_date.iloc[0]
        return group.iloc[0]

    df = df.groupby("Job Link", as_index=False).apply(best_row).reset_index(drop=True)


    # -------------------------
    # FILL LOCATION (scraper misses some)
    # -------------------------
    df["Location"] = df.apply(lambda r: extract_location_field(r["Job Title"], r["Location"]), axis=1)
    df["Location"] = df["Location"].apply(normalize_location_cell)


    # -------------------------
    # CLEAN TITLES AFTER LOCATION EXTRACTION (IMPORTANT)
    # -------------------------
    df["Job Title"] = df["Job Title"].apply(strip_location_from_title)
    df["Job Title"] = df["Job Title"].apply(lambda t: re.sub(r'\s{2,}', ' ', t).strip(" -,."))
    

    # -------------------------
    # CLASSIFY FUNCTION / SENIORITY / SKILLS
    # -------------------------
    df["Function"] = df["Job Title"].apply(classify_function)
    df["Seniority"] = df.apply(lambda r: classify_seniority(r["Job Title"], r["Location"]), axis=1)
    df["Skills_in_Title"] = df["Job Title"].apply(extract_skills)


    # -------------------------
    # NORMALIZE SENIORITY
    # -------------------------
    def normalize_sen(s):
        s = s or ""
        if not s:
            return "Unknown"
        s = re.sub(r'\bsr[\.\s]?\b', 'Senior', s, flags=re.I)
        if re.search(r'\b(principal|staff|architect)\b', s, re.I):
            return "Senior/Lead"
        if re.search(r'\b(director|vp|head|chief)\b', s, re.I):
            return "Director+"
        if re.search(r'\b(entry|junior|jr|graduate)\b', s, re.I):
            return "Entry"
        if re.search(r'\b(mid|intermediate|level ii)\b', s, re.I):
            return "Mid"
        return s

    df["Seniority"] = df["Seniority"].apply(normalize_sen)


    # -------------------------
    # RECOMPUTE DAYS SINCE POSTED
    # -------------------------
    def compute_days(pd_str):
        if not pd_str:
            return ""
        try:
            d = datetime.fromisoformat(pd_str).date()
            return (date.today() - d).days
        except:
            return ""

    df["Days Since Posted"] = df["Posting Date"].apply(compute_days)


    # -------------------------
    # OUTPUT
    # -------------------------
    df = df.sort_values(by=["Company", "Job Title"])
    df["Skills_in_Title"] = df["Skills_in_Title"].apply(lambda x: json.dumps(x))

    out_cols = [
        "Company","Job Title","Job Link","Location",
        "Posting Date","Days Since Posted",
        "Function","Seniority","Skills_in_Title"
    ]

    df[out_cols].to_csv("jobs_cleaned_final_enriched.csv", index=False)
    print("[OK] wrote jobs_cleaned_final_enriched.csv with", len(df), "rows")


if __name__ == "__main__":
    main()
