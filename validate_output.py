# validate_output.py  (Enterprise C+ Validator — FINAL)
import pandas as pd
import re
import sys

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
    r"press"
]
GARBAGE_RE = re.compile("|".join(GARBAGE_TITLE_PATTERNS), re.I)

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
    "download"
]

def fail(msg):
    print(f"❌ VALIDATION FAILED: {msg}")
    sys.exit(1)

def warn(msg):
    print(f"⚠️  WARNING: {msg}")

def ok(msg):
    print(f"✅ {msg}")

def main():
    try:
        df = pd.read_csv("jobs_cleaned_final_enriched.csv", dtype=str).fillna("")
    except Exception as e:
        fail(f"Could not read output CSV: {e}")

    print("======== ENTERPRISE VALIDATOR ========")
    print(f"Total rows: {len(df)}")

    # -----------------------------
    # 1. COLUMN CHECK
    # -----------------------------
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            fail(f"Missing column: {col}")
    ok("All required columns present")

    # -----------------------------
    # 2. DUPLICATE LINK CHECK
    # -----------------------------
    dupes = df["Job Link"][df["Job Link"].duplicated()]
    if len(dupes) > 0:
        warn(f"Duplicate job links found: {len(dupes)}")
    else:
        ok("No duplicate job links")

    # -----------------------------
    # 3. JUNK TITLE CHECK
    # -----------------------------
    garbage = df[df["Job Title"].str.lower().str.match(GARBAGE_RE)]
    if len(garbage) > 0:
        warn(f"Found garbage titles: {len(garbage)} (e.g. marketing pages, alerts, blog posts)")
    else:
        ok("No garbage titles detected")

    # -----------------------------
    # 4. LOCATION CHECK
    # -----------------------------
    missing_loc = df[df["Location"] == ""]
    pct_loc_missing = round(len(missing_loc) / len(df) * 100, 1)
    print(f"Missing Location: {len(missing_loc)} ({pct_loc_missing}%)")
    if pct_loc_missing > 35:
        warn("High missing location rate (>35%)")

    # -----------------------------
    # 5. POSTING DATE CHECK
    # -----------------------------
    missing_date = df[df["Posting Date"] == ""]
    pct_date_missing = round(len(missing_date) / len(df) * 100, 1)
    print(f"Missing Posting Date: {len(missing_date)} ({pct_date_missing}%)")
    if pct_date_missing > 60:
        warn("High missing posting date rate (>60%)")

    # -----------------------------
    # 6. SENIORITY CHECK
    # -----------------------------
    unknown_sen = df[df["Seniority"].str.lower() == "unknown"]
    pct_unknown_sen = round(len(unknown_sen) / len(df) * 100, 1)
    print(f"Unknown Seniority: {len(unknown_sen)} ({pct_unknown_sen}%)")
    if pct_unknown_sen > 35:
        warn("High unknown seniority rate (>35%)")

    # -----------------------------
    # 7. FUNCTION CHECK
    # -----------------------------
    empty_function = df[df["Function"] == ""]
    if len(empty_function) > 0:
        warn(f"Rows without function classification: {len(empty_function)}")
    else:
        ok("Function field OK")

    # -----------------------------
    # 8. SPAM / NON-JOB LINK CHECK
    # -----------------------------
    spam_count = 0
    for idx, row in df.iterrows():
        link = str(row["Job Link"]).lower()
        if any(w in link for w in ATS_SPAM_WORDS):
            if row["Job Title"] != "" and not re.search(r'engineer|manager|data|product|analyst|sales', row["Job Title"], re.I):
                spam_count += 1
    if spam_count > 0:
        warn(f"Detected potential non-job links: {spam_count}")
    else:
        ok("No ATS spam patterns detected")

    # -----------------------------
    # 9. FINAL PASS/FAIL
    # -----------------------------
    if pct_loc_missing <= 50 and pct_date_missing <= 70:
        ok("VALIDATION OK")
    else:
        warn("Validation passed BUT quality is low — consider new patches.")

    print("======================================")

if __name__ == "__main__":
    main()
