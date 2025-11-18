# validate_output.py
import pandas as pd, sys

df = pd.read_csv("jobs_cleaned_final_enriched.csv", dtype=str).fillna("")
total = len(df)
missing_loc = df["Location"].apply(lambda x: not bool(str(x).strip())).sum()
missing_date = df["Posting Date"].apply(lambda x: not bool(str(x).strip())).sum()
unknown_sen = (df["Seniority"].str.lower() == "unknown").sum()

print(f"Total rows: {total}")
print(f"Missing Location: {missing_loc} ({missing_loc/total:.1%})")
print(f"Missing Posting Date: {missing_date} ({missing_date/total:.1%})")
print(f"Unknown Seniority: {unknown_sen} ({unknown_sen/total:.1%})")

# thresholds - tune these if needed
if missing_loc/total > 0.40:
    print("FAIL: too many missing locations (>40%)")
    sys.exit(2)
if missing_date/total > 0.70:
    print("FAIL: too many missing posting dates (>70%)")
    sys.exit(3)
if unknown_sen/total > 0.30:
    print("WARN: many unknown seniorities (>30%)")
    # don't fail, just warn
print("VALIDATION OK")
