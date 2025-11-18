# clean_jobs_cplus.py  (ENTERPRISE C+ cleaner)
import pandas as pd
import re, json

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
    ("Mid", r"\b(mid|intermediate|experienced)\b"),
    ("Entry", r"\b(entry|junior|jr\.|graduate)\b"),
    ("Intern", r"\b(intern|trainee)\b"),
]

COMPANY_FIXES = {
    "Fivetran": ["fivetran", "fivetran - launchers"],
    "Ataccama": ["ataccama","atacama"],
    "Datadog": ["datadog"],
    # add mappings for weird names
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
    s = re.sub(r'[^\x00-\x7F]+',' ', s)  # strip weird unicode
    s = s.strip(" -:,.")
    return s

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
    # remove parenthetical location like "Title (London, Remote)"
    t = re.sub(r'\(([^)]+)\)\s*$', '', title).strip()
    # remove trailing " - Location" or " | Location"
    t = re.sub(r'\s+[-|]\s*[A-Za-z0-9 ,\-()\/]+$', '', t).strip()
    # remove comma separated location tail if short
    parts = re.split(r'\s{2,}| - | \| | — | – |,', title)
    if len(parts) > 1:
        tail = parts[-1]
        if len(tail.split()) <= 4 and re.search(r'[A-Za-z]', tail):
            t = " ".join(parts[:-1]).strip()
    return t

def extract_location_field(title, location_cell):
    # prefer explicit cell, else attempt parse
    if isinstance(location_cell, str) and location_cell.strip():
        return normalize_location_cell(location_cell)
    # parentheses
    m = re.search(r'\(([^)]+)\)\s*$', title)
    if m: return normalize_location_cell(m.group(1))
    # trailing " - Berlin" style
    parts = re.split(r'\s+-\s+|\s+\|\s+|\s+—\s+', title)
    if len(parts) > 1:
        cand = parts[-1]
        if len(cand.split()) <= 4 and re.search(r'[A-Za-z]', cand):
            return normalize_location_cell(cand)
    return ""

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
    found = [s for s in SKILL_WORDS if s in tl]
    return list(dict.fromkeys(found))

# ---------- MAIN ----------
def main():
    df = pd.read_csv("jobs_final_hard.csv", dtype=str).fillna("")
    # basic normalizations
    df["Company"] = df["Company"].apply(clean_text).apply(normalize_company)
    df["Job Title"] = df["Job Title"].apply(clean_text)
    df["Job Link"] = df["Job Link"].apply(lambda x: x.strip())
    # remove obviously bad titles
    df = df[~df["Job Title"].str.match(SKIP_TITLE_RE)]
    # company-specific link filters (skip marketing pages)
    def company_link_filter(row):
        comp = row["Company"].lower()
        link = str(row["Job Link"]).lower()
        text = row["Job Title"].lower()
        if "ataccama" in comp and ("one-team" in link or "one-team" in text or "team" in text and "careers" not in link):
            return False
        if "fivetran" in comp and ("launchers" in link or "launchers" in text or "developer-relations" in link):
            return False
        if "datadog" in comp and ("resources" in link or "events" in link or "blog" in link):
            return False
        return True
    df = df[df.apply(company_link_filter, axis=1)]

    # attempt to fill location if empty
    df["Location"] = df.apply(lambda r: extract_location_field(r["Job Title"], r.get("Location","")), axis=1)
    df["Location"] = df["Location"].apply(normalize_location_cell)

    # strip location fragments from title and normalize title
    df["Job Title"] = df.apply(lambda r: strip_location_from_title(r["Job Title"]), axis=1)
    df["Job Title"] = df["Job Title"].apply(lambda t: re.sub(r'\s{2,}', ' ', t).strip(" -,."))

    # classification
    df["Function"] = df["Job Title"].apply(classify_function)
    df["Seniority"] = df.apply(lambda r: classify_seniority(r["Job Title"], r["Location"]), axis=1)
    df["Skills_in_Title"] = df["Job Title"].apply(extract_skills)

    # normalize Seniority textual variants
    def normalize_sen(s):
        s = (s or "").strip()
        if not s: return "Unknown"
        s = re.sub(r'\bsr[\.\s]?\b', 'Senior', s, flags=re.I)
        s = re.sub(r'\b(principal|staff|architect)\b', 'Senior/Lead', s, flags=re.I)
        s = re.sub(r'\b(director|vp|vice president|head|chief)\b', 'Director+', s, flags=re.I)
        s = re.sub(r'\b(intern|graduate|trainee)\b', 'Intern', s, flags=re.I)
        if re.search(r'\b(senior|lead|principal|staff)\b', s, re.I): return "Senior/Lead"
        if re.search(r'\b(director|vp|head|chief)\b', s, re.I): return "Director+"
        if re.search(r'\b(entry|junior|jr|graduate)\b', s, re.I): return "Entry"
        if re.search(r'\b(mid|intermediate)\b', s, re.I): return "Mid"
        return "Unknown"
    df["Seniority"] = df["Seniority"].apply(normalize_sen)

    # dedupe and final formatting
    df = df.sort_values(by=["Company","Job Title"])
    df = df.drop_duplicates(subset=["Job Link"], keep="first")
    df["Skills_in_Title"] = df["Skills_in_Title"].apply(lambda x: json.dumps(x))
    out_cols = ["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Function","Seniority","Skills_in_Title"]
    df = df[out_cols]
    df.to_csv("jobs_cleaned_final_enriched.csv", index=False)
    print("[OK] wrote jobs_cleaned_final_enriched.csv with", len(df), "rows")

if __name__ == "__main__":
    main()
