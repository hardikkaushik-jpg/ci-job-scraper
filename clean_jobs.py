import pandas as pd
import re

# ------------ CONFIG ------------

FORBIDDEN_WORDS = [
    "privacy", "legal", "company", "about", "terms", "cookie",
    "subscribe", "newsletter", "view", "contact", "profile",
    "dashboard", "blog", "press", "team", "leadership"
]

FUNCTION_MAP = {
    "engineer": "Engineering",
    "developer": "Engineering",
    "sre": "Engineering",
    "qa": "Engineering",
    "scientist": "Data/AI",
    "analytics": "Data/AI",
    "analyst": "Data/AI",
    "sales": "Sales",
    "account": "Sales",
    "solutions": "Sales",
    "marketing": "Marketing",
    "product": "Product",
    "design": "Product",
    "manager": "Management",
    "director": "Management"
}

SENIORITY_PATTERNS = {
    "Intern": r"(intern|graduate)",
    "Entry": r"(entry|junior)",
    "Mid": r"(mid|intermediate)",
    "Senior/Lead": r"(senior|lead|sr)",
    "Director+": r"(director|vp|head|chief)"
}

SKILL_WORDS = [
    "python","sql","java","aws","azure","gcp","etl","spark","snowflake",
    "docker","kubernetes","airflow","ml","ai","tableau","dbt","rest"
]

# ------------ HELPERS ------------

def clean_title(t):
    if not isinstance(t, str):
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    for w in FORBIDDEN_WORDS:
        t = re.sub(fr"\b{w}\b", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -,")
    return t

def classify_function(title):
    tl = title.lower()
    for k,v in FUNCTION_MAP.items():
        if k in tl:
            return v
    return "Other"

def classify_seniority(title):
    tl = title.lower()
    for label,pattern in SENIORITY_PATTERNS.items():
        if re.search(pattern, tl):
            return label
    return "Unknown"

def extract_skills(title):
    tl = title.lower()
    found = [s for s in SKILL_WORDS if s in tl]
    return ", ".join(found)

# ------------ MAIN PROCESS ------------

def main():
    df = pd.read_csv("jobs_final_hard.csv")

    # 1. CLEAN TITLES
    df["Job Title"] = df["Job Title"].fillna("").apply(clean_title)

    # 2. REMOVE empty/useless
    df = df[df["Job Title"].str.len() > 2]

    # 3. FIX COMPANIES WITH NOISE
    df = df[~df["Company"].str.contains("attacama|ataccama bad page", case=False, na=False)]
    df = df[~df["Company"].str.contains("fivetran-launchers", case=False, na=False)]
    df = df[~df["Company"].str.contains("snaplogic.*resource", case=False, na=False)]

    # 4. EXTRACT FUNCTION + SENIORITY + SKILLS
    df["Function"] = df["Job Title"].apply(classify_function)
    df["Seniority"] = df["Job Title"].apply(classify_seniority)
    df["Skills_in_Title"] = df["Job Title"].apply(extract_skills)

    # 5. CLEAN LOCATION
    df["Location"] = df["Location"].fillna("").str.title()

    # 6. DEDUPE BY JOB LINK
    df = df.drop_duplicates(subset=["Job Link"])

    # 7. SORT CLEAN OUTPUT
    df = df.sort_values(["Company","Job Title"])

    df.to_csv("jobs_cleaned_final_enriched.csv", index=False)
    print("[OK] Enriched CSV created: jobs_cleaned_final_enriched.csv")

if __name__ == "__main__":
    main()
