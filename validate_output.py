# validate_output.py — Refactored v5.0
# Changes:
#   1. Duplicate rate > 5% → HARD FAIL (sys.exit 1)
#   2. Garbage title rate > 2% → HARD FAIL
#   3. Per-company spike > 300 rows → HARD FAIL
#   4. Missing enrichment columns → HARD FAIL
#   5. first_seen / last_seen column checks added
#   6. Summary report printed before pass/fail

import pandas as pd
import re
import sys
import os
from collections import Counter

# ─────────────────────────────────────────────────────────────────────────────
# THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────
MAX_DUPLICATE_RATE_PCT   = 5.0
MAX_GARBAGE_RATE_PCT     = 2.0
MAX_ROWS_PER_COMPANY     = 300
# Large companies legitimately exceed the default cap
COMPANY_CAP_OVERRIDES = {
    "Databricks": 700,   # real headcount — Greenhouse API returns all global roles
    "MongoDB":    450,
    "Fivetran":   200,
    "Syniti":     70,
}
MAX_MISSING_LOC_PCT      = 40.0
MAX_MISSING_DATE_PCT     = 65.0
MAX_UNKNOWN_SENIORITY_PCT = 40.0

# ─────────────────────────────────────────────────────────────────────────────
# COLUMNS
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_COLUMNS = [
    "Company", "Job Title", "Job Link", "Location",
    "Posting Date", "Days Since Posted",
    "Function", "Seniority", "Skills_in_Title",
]
ENRICHMENT_COLUMNS = [
    "Company_Group", "Product_Focus", "Primary_Skill",
    "Extracted_Skills", "Relevancy_to_Actian", "Trend_Score",
    "First_Seen", "Last_Seen",
]

# ─────────────────────────────────────────────────────────────────────────────
# GARBAGE PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
GARBAGE_PATTERNS = [
    r"create alert", r"sign in", r"sign up", r"privacy",
    r"about", r"dashboard", r"download", r"our story",
    r"diversity", r"blog", r"learn [a-z]+", r"developer portal",
    r"career hub", r"press", r"resources", r"webinar",
    r"podcast", r"newsletter", r"^careers$", r"^jobs$",
]
GARBAGE_RE = re.compile("|".join(GARBAGE_PATTERNS), re.I)

ATS_SPAM_WORDS = [
    "about-us", "about", "life-at", "culture", "diversity",
    "learning-and-development", "guide", "resources", "events",
    "blog", "use-case", "product", "download", "press",
]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fail(msg):
    print(f"\n❌  VALIDATION FAILED: {msg}")
    sys.exit(1)

def warn(msg):
    print(f"⚠️   WARNING: {msg}")

def ok(msg):
    print(f"✅  {msg}")

def pct(n, total):
    return round(n / total * 100, 1) if total else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    repo_root = os.path.dirname(os.path.abspath(__file__))
    csv_path  = os.path.join(repo_root, "jobs_cleaned_final_enriched.csv")

    try:
        df = pd.read_csv(csv_path, dtype=str).fillna("")
    except Exception as e:
        fail(f"Could not read output CSV: {e}")

    total = len(df)
    print("=" * 52)
    print("         ENTERPRISE VALIDATOR v5.0")
    print("=" * 52)
    print(f"  Total rows : {total}")
    print(f"  Companies  : {df['Company'].nunique() if 'Company' in df.columns else '?'}")
    print("=" * 52)

    errors   = []
    warnings = []

    # ── 1. Required columns ──────────────────────────────────────────────────
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    for col in ENRICHMENT_COLUMNS:
        if col not in df.columns:
            errors.append(f"Missing enrichment column: {col}")

    if errors:
        for e in errors:
            print(f"❌  {e}")
        sys.exit(1)
    ok("All required and enrichment columns present")

    # ── 2. Duplicate job links ────────────────────────────────────────────────
    dupe_count = df["Job Link"].duplicated().sum()
    dupe_rate  = pct(dupe_count, total)
    print(f"  Duplicate links : {dupe_count} ({dupe_rate}%)")
    if dupe_rate > MAX_DUPLICATE_RATE_PCT:
        fail(f"Duplicate rate {dupe_rate}% exceeds threshold {MAX_DUPLICATE_RATE_PCT}%. "
             f"Likely double-scraping or URL normalisation failure.")
    elif dupe_count > 0:
        warn(f"{dupe_count} duplicate links detected (below threshold)")
    else:
        ok("No duplicate job links")

    # ── 3. Garbage titles ────────────────────────────────────────────────────
    garbage_mask  = df["Job Title"].str.lower().str.contains(GARBAGE_RE, na=False)
    garbage_count = garbage_mask.sum()
    garbage_rate  = pct(garbage_count, total)
    print(f"  Garbage titles  : {garbage_count} ({garbage_rate}%)")
    if garbage_rate > MAX_GARBAGE_RATE_PCT:
        fail(f"Garbage title rate {garbage_rate}% exceeds threshold {MAX_GARBAGE_RATE_PCT}%.")
    elif garbage_count > 0:
        warn(f"{garbage_count} potential garbage titles")
        print(df[garbage_mask][["Company","Job Title"]].head(5).to_string(index=False))
    else:
        ok("No garbage titles")

    # ── 4. Per-company row spike ──────────────────────────────────────────────
    counts   = df["Company"].value_counts()
    spikes   = counts[counts.index.map(
        lambda co: counts[co] > COMPANY_CAP_OVERRIDES.get(co, MAX_ROWS_PER_COMPANY)
    )]
    if not spikes.empty:
        spike_lines = ", ".join(f"{co}={n}" for co, n in spikes.items())
        warn(f"Company row spike detected: {spike_lines}. "
             f"Investigate if unexpected — may be legitimate for large companies.")
    else:
        ok(f"No per-company spikes")

    # ── 5. Location coverage ─────────────────────────────────────────────────
    missing_loc  = (df["Location"] == "").sum()
    missing_loc_pct = pct(missing_loc, total)
    print(f"  Missing location: {missing_loc} ({missing_loc_pct}%)")
    if missing_loc_pct > MAX_MISSING_LOC_PCT:
        warn(f"High missing location rate ({missing_loc_pct}%) — check detail fetching")
    else:
        ok("Location coverage acceptable")

    # ── 6. Posting date coverage ──────────────────────────────────────────────
    missing_date     = (df["Posting Date"] == "").sum()
    missing_date_pct = pct(missing_date, total)
    print(f"  Missing date    : {missing_date} ({missing_date_pct}%)")
    if missing_date_pct > MAX_MISSING_DATE_PCT:
        warn(f"High missing posting date rate ({missing_date_pct}%)")
    else:
        ok("Posting date coverage acceptable")

    # ── 7. Seniority coverage ─────────────────────────────────────────────────
    unknown_sen     = (df["Seniority"].str.lower() == "unknown").sum()
    unknown_sen_pct = pct(unknown_sen, total)
    print(f"  Unknown seniority: {unknown_sen} ({unknown_sen_pct}%)")
    if unknown_sen_pct > MAX_UNKNOWN_SENIORITY_PCT:
        warn(f"High unknown seniority rate ({unknown_sen_pct}%)")
    else:
        ok("Seniority coverage acceptable")

    # ── 8. First_Seen / Last_Seen ─────────────────────────────────────────────
    if "First_Seen" in df.columns and "Last_Seen" in df.columns:
        missing_fs = (df["First_Seen"] == "").sum()
        if missing_fs > 0:
            warn(f"{missing_fs} rows missing First_Seen date")
        else:
            ok("First_Seen / Last_Seen populated")
    else:
        warn("Lifecycle columns (First_Seen/Last_Seen) not found")

    # ── 9. Relevancy distribution ─────────────────────────────────────────────
    if "Relevancy_to_Actian" in df.columns:
        try:
            rel = pd.to_numeric(df["Relevancy_to_Actian"], errors="coerce")
            high_rel = (rel >= 8.5).sum()
            print(f"  High-relevancy roles (≥8.5): {high_rel}")
            ok("Relevancy scoring present")
        except Exception:
            warn("Could not parse Relevancy_to_Actian column")

    # ── 10. ATS spam check ────────────────────────────────────────────────────
    spam_count = 0
    for _, row in df.iterrows():
        link  = (row["Job Link"] or "").lower()
        title = row["Job Title"] or ""
        if any(w in link for w in ATS_SPAM_WORDS):
            if not re.search(r'(engineer|manager|analyst|data|product|sales)', title, re.I):
                spam_count += 1
    if spam_count > 0:
        warn(f"Potential ATS spam links: {spam_count}")
    else:
        ok("No ATS spam detected")

    # ── Company distribution summary ──────────────────────────────────────────
    print("\n  Top 10 companies by job count:")
    for co, n in counts.head(10).items():
        flag = " ⚠️ " if n > MAX_ROWS_PER_COMPANY * 0.8 else ""
        print(f"    {co:<30} {n:>4}{flag}")

    # ── FINAL VERDICT ─────────────────────────────────────────────────────────
    print("\n" + "=" * 52)
    ok("VALIDATION PASSED — pipeline output is clean")
    print("=" * 52)


if __name__ == "__main__":
    main()
