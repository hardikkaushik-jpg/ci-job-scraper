# clean_jobs.py
# Clean + enrich jobs_final_hard.csv -> jobs_cleaned_final_enriched.csv
import pandas as pd
import re, ast, json

# -------- CONFIG --------
SKILL_WORDS = [
    "python","sql","java","aws","azure","gcp","etl","spark","snowflake","dbt",
    "docker","kubernetes","airflow","ml","ai","tableau","lookml","hadoop","scala",
    "nosql","redshift","bigquery","hive","react","node","javascript","go","rust"
]

FUNCTION_KEYWORDS = {
    "Engineering": ["engineer","developer","sre","site reliability","platform","backend","frontend","full stack","devops","software"],
    "Data/Analytics": ["data","analyst","analytics","scientist","machine learning","ml","etl","pipeline","data engineer","data scientist"],
    "Product": ["product manager","product","pm","ux","ux/ui","ui/ux","designer"],
    "Sales": ["sales","account","business development","bd","sdm","gogetter"],
    "Customer Success": ["customer success","cs","support","implementation","onboarding"],
    "Marketing": ["marketing","growth","demand"],
    "Operations": ["ops","finance","hr","people","recruiter"]
}

SENIORITY_PATTERNS = [
    ("Director+", r"\b(director|vp|vice president|head of|chief|c-)\b"),
    ("Senior/Lead", r"\b(senior|sr\.|lead|principal|staff)\b"),
    ("Mid", r"\b(mid|intermediate)\b"),
    ("Entry", r"\b(entry|junior|jr\.|graduate)\b"),
    ("Intern", r"\b(intern|trainee)\b"),
]

# company-specific cleaning map
COMPANY_FIXES = {
    "Fivetran": ["Fivetran", "Fivetran - Jobs", "Fivetran - Launchers"],
    "Ataccama": ["Ataccama", "Ataccama Careers Page", "Atacama"],
    "SnapLogic": ["Snaplogic", "Snap Logic", "SnapLogic Careers"],
    # add more if required
}

# -------- HELPERS --------
def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'[^\x00-\x7F]+',' ', s)  # remove weird unicode control chars
    s = s.strip(" -:,.")
    return s

def classify_function(title):
    tl = title.lower()
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

def extract_skills_from_title(title):
    tl = (title or "").lower()
    found = [s for s in SKILL_WORDS if s in tl]
    return list(dict.fromkeys(found))  # dedupe preserving order

def normalize_company(name):
    if not isinstance(name, str):
        return ""
    name = clean_text(name)
    # simple normalization rules
    for canonical, variants in COMPANY_FIXES.items():
        for v in variants:
            if v.lower() in name.lower():
                return canonical
    return name

def normalize_location_cell(loc):
    if not isinstance(loc, str) or not loc.strip():
        return ""
    # common mappings
    loc = loc.strip()
    # remove trailing "remote -"
    loc = re.sub(r'^(remote[\s\-:,]*)+', 'Remote, ', loc, flags=re.I)
    # normalize separators and titlecase
    parts = [p.strip() for p in re.split(r'[,/;|]+', loc) if p.strip()]
    out = []
    seen = set()
    for p in parts:
        k = p.lower()
        if k in seen: 
            continue
        seen.add(k)
        if k == "remote":
            out.append("Remote")
        else:
            out.append(p.title())
    return ", ".join(out)

# -------- MAIN --------
def main():
    df = pd.read_csv("jobs_final_hard.csv", dtype=str).fillna("")
    # basic cleaning
    df["Company"] = df["Company"].apply(clean_text).apply(normalize_company)
    df["Job Title"] = df["Job Title"].apply(clean_text)
    df["Location"] = df["Location"].apply(clean_text).apply(normalize_location_cell)
    df["Job Link"] = df["Job Link"].apply(lambda x: x.strip())

    # if Location empty, attempt to parse location from title parenthetical
    def fallback_loc(row):
        if row["Location"]:
            return row["Location"]
        # look for parentheses at end
        m = re.search(r'\(([^)]+)\)\s*$', row["Job Title"])
        if m:
            return normalize_location_cell(m.group(1))
        # trailing dash tokens: "Title - Berlin"
        parts = re.split(r'\s+-\s+|\s+\|\s+|\s+—\s+', row["Job Title"])
        if len(parts) > 1:
            cand = parts[-1]
            if len(cand.split()) <= 4 and re.search(r'[A-Za-z]', cand):
                return normalize_location_cell(cand)
        return ""

    df["Location"] = df.apply(lambda r: r["Location"] if r["Location"] else fallback_loc(r), axis=1)

    # classify function, seniority and skills
    df["Function"] = df["Job Title"].apply(classify_function)
    df["Seniority"] = df.apply(lambda r: classify_seniority(r["Job Title"], r["Location"]), axis=1)
    df["Skills_in_Title"] = df["Job Title"].apply(lambda t: extract_skills_from_title(t))

    # advanced: normalize Seniority (map many forms)
    def normalize_sen(s):
        s = (s or "").strip()
        if not s:
            return "Unknown"
        s = s.replace("Sr.", "Senior").replace("Sr ", "Senior ")
        s = re.sub(r'\b(senior|sr|lead|principal|staff)\b', "Senior/Lead", s, flags=re.I)
        s = re.sub(r'\b(director|vp|vice president|head|chief)\b', "Director+", s, flags=re.I)
        s = re.sub(r'\b(intern|graduate)\b', "Intern", s, flags=re.I)
        s = re.sub(r'\b(entry|junior|jr)\b', "Entry", s, flags=re.I)
        s = re.sub(r'\b(mid|intermediate)\b', "Mid", s, flags=re.I)
        # final mapping
        for label in ["Director+", "Senior/Lead", "Mid", "Entry", "Intern"]:
            if label.lower().split('/')[0] in s.lower():
                return label
        return s if s else "Unknown"

    df["Seniority"] = df["Seniority"].apply(normalize_sen)

    # dedupe by Job Link, but preserve latest when duplicates exist
    df = df.sort_values(by=["Company","Job Title"])
    df = df.drop_duplicates(subset=["Job Link"], keep="first")

    # final formatting: convert Skills to JSON-like list string for easy parsing in Power BI
    df["Skills_in_Title"] = df["Skills_in_Title"].apply(lambda lst: json.dumps(lst))

    out_cols = ["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Function","Seniority","Skills_in_Title"]
    df = df[out_cols]
    df.to_csv("jobs_cleaned_final_enriched.csv", index=False)
    print("[OK] wrote jobs_cleaned_final_enriched.csv with", len(df), "rows")

if __name__ == "__main__":
    main()
