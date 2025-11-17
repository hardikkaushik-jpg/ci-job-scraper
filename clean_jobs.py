# clean_jobs.py
"""
Cleans the raw CSV "jobs_final_hard.csv" into "jobs_cleaned.csv" and writes a short analysis "jobs_analysis.txt".
Usage:
  pip install -r requirements.txt
  python clean_jobs.py
"""
import re, sys, csv
from datetime import datetime, date
import pandas as pd
from dateutil import parser as dateparser

INFILE = "jobs_final_hard.csv"            # input file you uploaded
OUTFILE = "jobs_cleaned.csv"   # cleaned output
REPORT = "jobs_analysis.txt"

# Forbidden phrases that indicate non-job rows (case-insensitive)
FORBIDDEN_TITLE_PATTERNS = [
    r'learn\s*more', r'apply\s*now', r'view\s+all', r'view\s+openings', r'product\b',
    r'solutions?', r'connectors?', r'features?', r'pricing', r'privacy', r'cookie',
    r'legal', r'contact', r'docs?', r'resources?', r'home', r'careers\b', r'career page',
    r'profile', r'help', r'press', r'events?', r'news', r'blog\b', r'language', r'lang:',
    r'deutsch', r'fran[çc]ais', r'italiano', r'portugu[eê]s', r'日本語', r'한국어', r'简体中文',
]

IMAGE_EXT = re.compile(r'\.(jpg|jpeg|png|gif|svg|webp)$', re.I)
JOB_LINK_POSITIVES = [
    'jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'greenhouse',
    'lever.co', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'boards.greenhouse',
]

LOCATION_HINTS_RE = re.compile(
    r'\b(remote|hybrid|onsite|home|work from home|usa|united states|uk|united kingdom|india|germany|france|canada|singapore|london|new york|berlin|paris|hyderabad|bengaluru|chennai|atlanta|zurich|amsterdam|dublin)\b',
    re.I
)

def normalize_title(t):
    if not isinstance(t, str):
        return ""
    s = t.replace("\r"," ").replace("\n"," ").strip()
    # remove weird spaces, NBSP etc.
    s = re.sub(r'[\u00A0\u200B]+',' ', s)
    # remove multiple spaces
    s = re.sub(r'\s{2,}', ' ', s)
    # remove trailing separators
    s = s.strip(" -:,.")
    return s

def looks_like_nonjob(title, link):
    t = (title or "").lower()
    # link looks like an image or static
    if link and IMAGE_EXT.search(link):
        return True
    # forbidden phrases in title
    for p in FORBIDDEN_TITLE_PATTERNS:
        if re.search(p, t, re.I):
            return True
    # if there are words like "how it works", "product", "demo" -> non-job
    if re.search(r'\b(demo|how it works|download|documentation|case study|solution|feature)\b', t, re.I):
        return True
    # if link clearly is marketing but anchor text not role-like
    lowlink = (link or "").lower()
    if lowlink and any(x in lowlink for x in ['/product', '/features', '/pricing', '/docs', '/resources', '/legal', '/contact', '/about']):
        # allow if title strongly looks like a real role
        if not re.search(r'\b(engineer|developer|manager|analyst|architect|scientist|consultant|director|designer|sales|sre|qa|product|account executive|associate)\b', t, re.I):
            return True
    return False

def extract_location_from_title(title):
    if not isinstance(title, str) or not title:
        return ""
    # look for parentheses or dash separated location like "Role — Location" or "Role (Location)" or lines
    # common formats:
    # "Role - City, Country", "Role (City, Country)", "Role \n City, Country"
    # try parentheses
    m = re.search(r'\(([^)]+)\)', title)
    if m:
        part = m.group(1).strip()
        # filter out language labels
        if len(part) <= 60 and LOCATION_HINTS_RE.search(part):
            return part
    # try " — " or " - " separators
    parts = re.split(r'[\u2013\u2014\-–—]\s*', title)
    if len(parts) >= 2:
        maybe_loc = parts[-1]
        if LOCATION_HINTS_RE.search(maybe_loc):
            return maybe_loc.strip()
    # try comma at end e.g., "Role, City"
    cands = title.split(',')
    if len(cands) >= 2:
        last = ','.join(cands[-2:]).strip()
        if LOCATION_HINTS_RE.search(last):
            return last
    # try newline in original (some scrapers put location in new line)
    lines = title.splitlines()
    if len(lines) >= 2:
        if LOCATION_HINTS_RE.search(lines[-1]):
            return lines[-1].strip()
    # fallback empty
    return ""

def extract_date_iso(s):
    if not isinstance(s, str) or not s:
        return ""
    s = s.strip()
    # ISO present
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if m:
        return m.group(1)
    # "Posted 21 days ago" -> convert
    m2 = re.search(r'posted\s+(\d+)\s+days?\s+ago', s, re.I)
    if m2:
        days = int(m2.group(1))
        d = date.today() - pd.Timedelta(days=days)
        return d.date().isoformat()
    # try parse with dateutil
    try:
        dt = dateparser.parse(s, fuzzy=True, default=datetime(2000,1,1))
        if dt.year > 2005:
            return dt.date().isoformat()
    except Exception:
        pass
    return ""

def main():
    import pandas as pd
    try:
        df = pd.read_csv(INFILE, dtype=str, keep_default_na=False)
    except Exception as e:
        print(f"ERROR: could not open {INFILE} -> {e}")
        sys.exit(1)

    # Standardize column names (tolerant)
    cols = {c.lower(): c for c in df.columns}
    def colname(pref):
        for k,v in cols.items():
            if pref in k:
                return v
        return None

    company_col = colname('company') or 'Company'
    title_col = colname('job title') or 'Job Title'
    link_col = colname('job link') or 'Job Link'
    location_col = colname('location') or 'Location'
    date_col = colname('posting date') or 'Posting Date'

    # ensure columns exist
    for c in [company_col, title_col, link_col]:
        if c not in df.columns:
            print(f"ERROR: expected column {c} missing in {INFILE}. Columns: {df.columns.tolist()}")
            sys.exit(1)

    # Normalize
    df[title_col] = df[title_col].fillna("").astype(str).map(normalize_title)
    df[link_col] = df[link_col].fillna("").astype(str).map(lambda x: x.strip())
    df[location_col] = df.get(location_col, "").fillna("").astype(str).map(lambda x: x.strip())

    initial_count = len(df)

    # Attempt to extract location from title where Location column missing/empty
    def ensure_location(row):
        loc = str(row.get(location_col,"")).strip()
        t = str(row.get(title_col,""))
        if loc:
            return loc
        extracted = extract_location_from_title(t)
        if extracted:
            # clean title by removing the extracted part
            cleaned_title = t.replace(extracted, "").strip(" ,-–—()")
            row[title_col] = re.sub(r'\s{2,}', ' ', cleaned_title)
            return extracted
        # also try patterns like "Role\nLocation"
        if '\n' in t:
            parts = [p.strip() for p in t.splitlines() if p.strip()]
            if len(parts) >= 2 and LOCATION_HINTS_RE.search(parts[-1]):
                row[title_col] = parts[0]
                return parts[-1]
        return ""

    df[location_col] = df.apply(ensure_location, axis=1)

    # Remove rows with empty link or link equals company career page (heuristic)
    df = df[df[link_col].str.strip() != ""].copy()

    # Remove rows that are clearly non-job based on title or link
    mask_nonjob = df.apply(lambda r: looks_like_nonjob(r[title_col], r[link_col]), axis=1)
    removed_nonjob = df[mask_nonjob].copy()
    df = df[~mask_nonjob].copy()

    # Further filter: drop rows where Job Link points to marketing/product unless title is strongly role-like
    def strong_role(t):
        return bool(re.search(r'\b(engineer|developer|manager|analyst|architect|scientist|consultant|director|designer|sales|sre|qa|product|account executive|associate)\b', str(t), re.I))
    def keep_link(row):
        link = row[link_col].lower()
        if any(x in link for x in ['/product','/features','/pricing','/docs','/resources','/legal','/contact']):
            return strong_role(row[title_col])
        if IMAGE_EXT.search(link):
            return False
        # if link is site root or careers landing and title short & role-like allow, else drop
        parsed = link.rstrip('/').lower()
        if parsed.endswith('/careers') or parsed.endswith('/careers') or parsed.endswith('/company/careers') or parsed.endswith('/careers/'):
            # allow only if title is clearly role-like and not vague
            return strong_role(row[title_col]) and len(row[title_col].split()) <= 8
        return True

    keep_mask = df.apply(keep_link, axis=1)
    removed_marketing = df[~keep_mask].copy()
    df = df[keep_mask].copy()

    # Normalize titles: remove language tags, "Learn More", "Apply", "Read More", "Learn More & Apply", trailing locations etc.
    def final_title_cleanup(t):
        s = re.sub(r'learn\s*more.*', '', t, flags=re.I)
        s = re.sub(r'learn\s*more\s*&?\s*apply.*', '', s, flags=re.I)
        s = re.sub(r'learn more & apply', '', s, flags=re.I)
        s = re.sub(r'\b(learn more|apply|read more|view all|view openings)\b', '', s, flags=re.I)
        s = re.sub(r'\b(deutsch|français|italiano|日本語|português|portuguese|español|korean|한국어|简体中文)\b', '', s, flags=re.I)
        s = re.sub(r'\s{2,}', ' ', s).strip(" -,.:;")
        return s

    df[title_col] = df[title_col].map(final_title_cleanup)

    # Posting date normalization (if Posting Date column present)
    if date_col in df.columns:
        df[date_col] = df[date_col].fillna("").astype(str).map(lambda x: extract_date_iso(x) or "")
    else:
        df[date_col] = ""

    # days since
    def days_since(s):
        if not s:
            return ""
        try:
            d = date.fromisoformat(s)
            return (date.today() - d).days
        except Exception:
            return ""
    df['Days Since Posted'] = df[date_col].map(days_since)

    # dedupe by Job Link keeping first occurrence
    df = df.drop_duplicates(subset=[link_col], keep='first').copy()

    # Summaries
    total_after = len(df)
    companies_count = df[company_col].value_counts().to_dict()
    removed_summary = {
        'nonjob_count': len(removed_nonjob),
        'marketing_count': len(removed_marketing)
    }

    # Save cleaned CSV
    df.to_csv(OUTFILE, index=False, quoting=csv.QUOTE_MINIMAL)
    # Save analysis report
    with open(REPORT, 'w', encoding='utf-8') as fh:
        fh.write(f"Input file: {INFILE}\n")
        fh.write(f"Initial rows: {initial_count}\n")
        fh.write(f"Rows after cleaning: {total_after}\n")
        fh.write("Removed rows summary:\n")
        fh.write(f"  - non-job rows removed (forbidden patterns): {removed_summary['nonjob_count']}\n")
        fh.write(f"  - marketing/product rows removed: {removed_summary['marketing_count']}\n")
        fh.write("\nJobs per company (top 50):\n")
        for comp, cnt in list(companies_count.items())[:200]:
            fh.write(f"{comp}: {cnt}\n")

    print("CLEANING COMPLETE")
    print(f"Initial rows: {initial_count}, after cleaning: {total_after}")
    print(f"Removed non-job rows: {removed_summary['nonjob_count']}, removed marketing rows: {removed_summary['marketing_count']}")
    print(f"Output: {OUTFILE}, Report: {REPORT}")
    print("Top companies in cleaned output:")
    for k,v in list(companies_count.items())[:30]:
        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()
