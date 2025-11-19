# validate_output.py  (Enterprise C+ Validator — 4.0 SAFE UPGRADE)

import pandas as pd
import re
import sys
import os

# -------------------------------------------------------------------
# Required downstream dashboard-safe columns
# -------------------------------------------------------------------
REQUIRED_COLUMNS = [
    "Company",
    "Job Title",
    "Job Link",
    "Location",
    "Posting Date",
    "Days Since Posted",
    "Function",
    "Seniority",
    "Skills_in_Title"
]

# Additional enrichment columns (OPTIONAL, warn if missing)
OPTIONAL_ENRICH_COLS = [
    "Company_Group",
    "Product_Focus",
    "Primary_Skill",
    "Extracted_Skills",
    "Relevancy_to_Actian",
    "AI_Focus",
    "Connector_Focus",
    "Trend_Score"
]

# -------------------------------------------------------------------
# Garbage title detector (expanded)
# -------------------------------------------------------------------
GARBAGE_TITLE_PATTERNS = [
    r"create alert",
    r"sign in",
    r"sign up",
    r"privacy",
    r"about",
    r"dashboard",
    r"download",
    r"our story",
    r"diversity",
    r"blog",
    r"learn [a-z]+",
    r"developer portal",
    r"career hub",
    r"press",
    r"resources",
    r"webinar",
    r"podcast",
    r"newsletter"
]
GARBAGE_RE = re.compile("|".join(GARBAGE_TITLE_PATTERNS), re.I)

# -------------------------------------------------------------------
# ATS spam indicators
# -------------------------------------------------------------------
ATS_SPAM_WORDS = [
    "about-us",
    "about",
    "life-at",
    "culture",
    "diversity",
    "learning-and-development",
    "guide",
    "resources",
    "events",
    "blog",
    "use-case",
    "product",
    "download",
    "press"
]

def fail(msg):
    print(f"❌ VALIDATION FAILED: {msg}")
    sys.exit(1)

def warn(msg):
    print(f"⚠️  WARNING: {msg}")

def ok(msg):
    print(f"✅ {msg}")

def main():
    # -------------------------------------------------------------------
    # Read CSV safely from repo root
    # -------------------------------------------------------------------
    repo_root = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(repo_root, "jobs_cleaned_final_enriched.csv")

    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
    except Exception as e:
        fail(f"Could not read output CSV at '{csv_path}': {e}")

    print("======== ENTERPRISE VALIDATOR ========")
    print(f"Total rows: {len(df)}")

    # -------------------------------------------------------------------
    # 1. REQUIRED COLUMN CHECK
    # -------------------------------------------------------------------
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            fail(f"Missing required column: {col}")
    ok("All required columns present")

    # -------------------------------------------------------------------
    # 2. OPTIONAL ENRICHMENT COLUMNS
    # -------------------------------------------------------------------
    for col in OPTIONAL_ENRICH_COLS:
        if col not in df.columns:
            warn(f"Missing enrichment column: {col}")
    if all(c in df.columns for c in OPTIONAL_ENRICH_COLS):
        ok("All enrichment columns present")

    # -------------------------------------------------------------------
    # 3. DUPLICATE LINK CHECK
    # -------------------------------------------------------------------
    dupes = df["Job Link"][df["Job Link"].duplicated()]
    if len(dupes) > 0:
        warn(f"Duplicate job links: {len(dupes)}")
    else:
        ok("No duplicate job links")

    # -------------------------------------------------------------------
    # 4. GARBAGE / NON-JOB TITLE CHECK
    # -------------------------------------------------------------------
    garbage = df[df["Job Title"].str.lower().str.contains(GARBAGE_RE)]
    if len(garbage) > 0:
        warn(f"Garbage titles detected: {len(garbage)}")
    else:
        ok("No garbage titles detected")

    # -------------------------------------------------------------------
    # 5. LOCATION CHECK
    # -------------------------------------------------------------------
    missing_loc = df[df["Location"] == ""]
    pct_loc_missing = round(len(missing_loc) / len(df) * 100, 1)
    print(f"Missing Location: {len(missing_loc)} ({pct_loc_missing}%)")
    if pct_loc_missing > 35:
        warn("High missing location rate (>35%)")

    # -------------------------------------------------------------------
    # 6. POSTING DATE CHECK
    # -------------------------------------------------------------------
    missing_date = df[df["Posting Date"] == ""]
    pct_date_missing = round(len(missing_date) / len(df) * 100, 1)
    print(f"Missing Posting Date: {len(missing_date)} ({pct_date_missing}%)")
    if pct_date_missing > 60:
        warn("High missing posting date rate (>60%)")

    # -------------------------------------------------------------------
    # 7. SENIORITY CHECK
    # -------------------------------------------------------------------
    unknown_sen = df[df["Seniority"].str.lower() == "unknown"]
    pct_unknown_sen = round(len(unknown_sen) / len(df) * 100, 1)
    print(f"Unknown Seniority: {len(unknown_sen)} ({pct_unknown_sen}%)")
    if pct_unknown_sen > 35:
        warn("High unknown seniority rate (>35%)")

    # -------------------------------------------------------------------
    # 8. FUNCTION CHECK
    # -------------------------------------------------------------------
    empty_function = df[df["Function"] == ""]
    if len(empty_function) > 0:
        warn(f"Rows without function classification: {len(empty_function)}")
    else:
        ok("Function field OK")

    # -------------------------------------------------------------------
    # 9. ATS SPAM CHECK
    # -------------------------------------------------------------------
    spam_count = 0
    for _, row in df.iterrows():
        link = row["Job Link"].lower()
        title = row["Job Title"]

        if any(w in link for w in ATS_SPAM_WORDS):
            if not re.search(r'(engineer|manager|analyst|data|product|sales)', title, re.I):
                spam_count += 1

    if spam_count > 0:
        warn(f"Potential non-job ATS spam links: {spam_count}")
    else:
        ok("No ATS spam detected")

    # -------------------------------------------------------------------
    # 10. FINAL PASS/FAIL
    # -------------------------------------------------------------------
    if pct_loc_missing <= 50 and pct_date_missing <= 70:
        ok("VALIDATION OK")
    else:
        warn("Validation passed BUT quality is low — consider scraper patches.")

    print("======================================")

if __name__ == "__main__":
    main()
