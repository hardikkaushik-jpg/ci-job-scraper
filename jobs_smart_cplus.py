#!/usr/bin/env python3
"""
jobs_scraper_cleaner_final.py
Combined improved scraper + cleaner for job listings.

Usage:
  - Scrape fresh: python3 jobs_scraper_cleaner_final.py --scrape
  - Clean existing: python3 jobs_scraper_cleaner_final.py --clean
Requires: playwright, beautifulsoup4, pandas
Install playwright and browsers:
  pip install playwright beautifulsoup4 pandas
  playwright install
"""

import re, json, time, sys, argparse
from datetime import datetime, date, timedelta
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# Optional: for cleaning stage
try:
    import pandas as pd
except:
    pd = None

# ---- CONFIG ----
COMPANIES = {
    "Airtable": ["https://airtable.com/careers#open-positions"],
    "Alation": ["https://alation.wd503.myworkdayjobs.com/ExternalSite"],
    "Alteryx": ["https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers"],
    "Ataccama": ["https://jobs.ataccama.com/#one-team"],
    "Atlan": ["https://atlan.com/careers"],
    "Anomalo": ["https://boards.greenhouse.io/anomalojobs"],
    "BigEye": ["https://www.bigeye.com/careers#positions"],
    "Boomi": ["https://boomi.com/company/careers/#greenhouseapp"],
    "CastorDoc (Coalesce)": ["https://jobs.ashbyhq.com/coalesce"],
    "Cloudera": ["https://cloudera.wd5.myworkdayjobs.com/External_Career"],
    "Collibra": ["https://www.collibra.com/company/careers#sub-menu-find-jobs"],
    "Couchbase": ["https://www.couchbase.com/careers/"],
    "Data.World": ["https://data.world/company/careers/#careers-list"],
    "Databricks": ["https://www.databricks.com/company/careers/open-positions"],
    "Datadog": ["https://careers.datadoghq.com/all-jobs/"],
    "DataGalaxy": ["https://www.welcometothejungle.com/en/companies/datagalaxy/jobs"],
    "Decube": ["https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41"],
    "Exasol": ["https://careers.exasol.com/en/jobs"],
    "Firebolt": ["https://www.firebolt.io/careers"],
    "Fivetran": ["https://www.fivetran.com/careers#jobs"],
    "GoldenSource": ["https://www.thegoldensource.com/careers/"],
    "InfluxData": ["https://www.influxdata.com/careers/#jobs"],
    "Informatica": ["https://informatica.gr8people.com/jobs"],
    "MariaDB": ["https://job-boards.eu.greenhouse.io/mariadbplc"],
    "Matillion": ["https://jobs.lever.co/matillion"],
    "MongoDB": [
        "https://www.mongodb.com/company/careers/teams/engineering",
        "https://www.mongodb.com/company/careers/teams/marketing",
        "https://www.mongodb.com/company/careers/teams/sales",
        "https://www.mongodb.com/company/careers/teams/product-management-and-design"
    ],
    "Monte Carlo": ["https://jobs.ashbyhq.com/montecarlodata"],
    "Mulesoft": ["https://www.mulesoft.com/careers"],
    "Nutanix": ["https://careers.nutanix.com/en/jobs/"],
    "OneTrust": ["https://www.onetrust.com/careers/"],
    "Oracle": ["https://careers.oracle.com/en/sites/jobsearch/jobs"],
    "Panoply (Sqream)": ["https://sqream.com/careers/"],
    "Precisely": [
        "https://www.precisely.com/careers-and-culture/us-jobs",
        "https://www.precisely.com/careers-and-culture/international-jobs"
    ],
    "Qlik": ["http://careerhub.qlik.com/careers"],
    "Sifflet": ["https://www.welcometothejungle.com/en/companies/sifflet/jobs"],
    "SnapLogic": ["https://www.snaplogic.com/company/careers/job-listings"],
    "Snowflake": ["https://careers.snowflake.com/global/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"]
}

PAGE_NAV_TIMEOUT = 40000
PAGE_DOM_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.12
MAX_DETAIL_FETCH = 5000

# heuristics
IMAGE_EXT_RE = re.compile(r'\.(jpg|jpeg|png|gif|svg|webp)$', re.I)
ROLE_WORDS_RE = re.compile(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|specialist|intern)\b', re.I)
ATS_DOMAINS = ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "jobs.lever", "gr8people", "workable", "job-boards")
NON_JOB_HINTS = re.compile(r'\b(blog|press|news|product|resource|case study|whitepaper|read more|webinar|events|privacy|terms|contact|about|insights|newsletter|download|guide|solution|features)\b', re.I)

SKILL_WORDS = [
    "python","sql","java","aws","azure","gcp","etl","spark","snowflake","dbt",
    "docker","kubernetes","airflow","ml","ai","tableau","hadoop","scala",
    "nosql","redshift","bigquery","hive","react","node","javascript","go","rust"
]

MONTH_MAP = {m: i for i, m in enumerate(["","jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}  # simple

# ---- HELPERS ----
def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    parsed = urlparse(href)
    if parsed.netloc:
        return href
    try:
        return urljoin(base, href)
    except:
        return href

def likely_job_anchor(href, text):
    if not href and not text:
        return False
    if href and IMAGE_EXT_RE.search(href):
        return False
    low = (text or href or "").lower()
    # skip obvious non-job anchors
    if NON_JOB_HINTS.search(low):
        # but allow if text contains role words AND it's ATS domain link
        if ROLE_WORDS_RE.search(low) and any(d in href.lower() for d in ATS_DOMAINS):
            return True
        return False
    # prefer anchors that mention roles or ATS / job path patterns
    positives = ["jobs","/job/","/jobs/","careers","open-positions","openings","greenhouse","lever.co","myworkdayjobs","bamboohr","ashby","job-boards","workable"]
    if any(p in (href or "").lower() for p in positives):
        return True
    if ROLE_WORDS_RE.search(text or "") and len((text or "").strip()) < 240:
        return True
    return False

def clean_title(raw):
    if not raw: return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = re.sub(r'learn more.*$', '', t, flags=re.I)
    t = re.sub(r'\s+[-|]\s*$', '', t)
    # remove trailing "(Remote)" and similar
    t = re.sub(r'\((remote|hybrid|[A-Za-z0-9 ,.-/]+)\)\s*$', '', t, flags=re.I)
    # remove leading bullets/nums
    t = re.sub(r'^\s*[\d\-\•\*\.\)]\s*', '', t)
    # remove "at Company" suffix
    t = re.sub(r'\s+at\s+[A-Z][A-Za-z0-9&\-\s]+$', '', t)
    return t.strip(" -,:;.|")

def parse_iso_date_like(s):
    # try iso
    try:
        return datetime.fromisoformat(s.split("T")[0]).date().isoformat()
    except:
        pass
    # try yyyy-mm-dd
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if m:
        return m.group(1)
    # try "Posted on January 2, 2024"
    m = re.search(r'(?:posted[:\s]*on\s*|posted[:\s]*)?([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})', s, re.I)
    if m:
        mon = m.group(1)[:3].lower()
        day = int(m.group(2)); year = int(m.group(3))
        mm = MONTH_MAP.get(mon, None)
        if mm:
            try:
                return date(year, mm, day).isoformat()
            except:
                pass
    # "X days ago"
    m = re.search(r'posted\s+(\d+)\s+days?\s+ago', s, re.I)
    if m:
        days = int(m.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    return ""

def extract_skills_from_text(txt):
    if not txt:
        return []
    low = txt.lower()
    found = [k for k in SKILL_WORDS if re.search(r'\b' + re.escape(k) + r'\b', low)]
    # keep order unique
    seen = set(); out = []
    for f in found:
        if f not in seen:
            out.append(f); seen.add(f)
    return out

def detect_seniority(title, description=""):
    t = (title or "").lower() + " " + (description or "").lower()
    if re.search(r'\b(chief|cto|ceo|cfo|coo|vp\b|vice president|sv[p|p]|executive director|managing director)\b', t):
        return "Director+"
    if re.search(r'\b(principal|staff|distinguished|fellow)\b', t):
        return "Principal/Staff"
    if re.search(r'\b(senior|sr\.|lead|team lead|principal)\b', t):
        return "Senior"
    if re.search(r'\b(manager|mgr|management)\b', t):
        return "Manager"
    if re.search(r'\b(mid |mid-|intermediate|associate|ii\b|2\b)\b', t):
        return "Mid"
    if re.search(r'\b(junior|jr\.|entry|graduate|trainee)\b', t):
        return "Entry"
    if re.search(r'\b(intern|internship|working student|werkstudent)\b', t):
        return "Intern"
    return "Unknown"

# ---- SCRAPER (Playwright + bs4) ----
def run_scraper(output_csv="jobs_final_hard.csv"):
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    rows = []
    detail_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context()
        page = ctx.new_page()

        for company, urls in COMPANIES.items():
            for main_url in urls:
                print("[SCRAPE] ", company, main_url)
                try:
                    page.goto(main_url, timeout=PAGE_NAV_TIMEOUT, wait_until="domcontentloaded")
                    page.wait_for_timeout(250)
                    listing_html = page.content()
                except Exception as e:
                    print("  [WARN] could not fetch listing:", e)
                    continue

                soup = BeautifulSoup(listing_html, "lxml")
                candidates = []
                # collect anchors
                for a in soup.find_all("a", href=True):
                    href = normalize_link(main_url, a.get("href"))
                    text = a.get_text(" ", strip=True) or ""
                    if likely_job_anchor(href, text):
                        candidates.append((href, text, a))
                # also scan job-card-ish containers
                for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"):
                    a = el.find("a", href=True)
                    text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if likely_job_anchor(href, text):
                        candidates.append((href, text, el))

                # dedupe
                seen = set(); filtered = []
                for href, text, el in candidates:
                    if not href: continue
                    if href in seen: continue
                    seen.add(href)
                    # skip mailto and anchors to same page
                    if href.startswith("mailto:") or href.startswith("tel:"): continue
                    # skip obvious product/resource pages by URL path if it contains keywords unless anchor looks strongly like role/ATS
                    low_href = href.lower()
                    if NON_JOB_HINTS.search(low_href) and not any(d in low_href for d in ATS_DOMAINS) and not ROLE_WORDS_RE.search(text):
                        continue
                    filtered.append((href, text, el))

                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    title_raw = anchor_text or ""
                    title_raw = clean_title(title_raw)
                    location_guess = ""
                    posting_date = ""
                    description_text = ""
                    must_fetch_detail = False

                    # decide whether to open detail: if ATS domain, or missing location/date, or title short, or link path contains /job/
                    if any(d in (link or "").lower() for d in ATS_DOMAINS):
                        must_fetch_detail = True
                    if not location_guess or True:  # we generally want detail to get description
                        must_fetch_detail = True
                    if "/job/" in (link or "").lower() or "/jobs/" in (link or "").lower() or "careers" in (link or "").lower():
                        must_fetch_detail = True

                    if must_fetch_detail and detail_count < MAX_DETAIL_FETCH:
                        detail_count += 1
                        try:
                            page.goto(link, timeout=PAGE_NAV_TIMEOUT, wait_until="domcontentloaded")
                            page.wait_for_timeout(300)
                            detail_html = page.content()
                        except Exception as e:
                            print("  [WARN] detail fetch failed:", e)
                            detail_html = ""

                        if detail_html:
                            s = BeautifulSoup(detail_html, "lxml")

                            # title from h1 if exists and is better
                            h1 = s.find("h1")
                            if h1 and h1.get_text(strip=True):
                                newt = clean_title(h1.get_text(" ", strip=True))
                                if len(newt) > 1:
                                    title_raw = newt

                            # description heuristics: look for role/description blocks, fallback to whole body text
                            desc_selectors = [
                                ".description", ".job-description", ".jobDesc", ".job-body", ".posting-candidate-wrapper",
                                ".content", "#job-description", ".application__content", ".jobPosting", ".jd"
                            ]
                            found_desc = None
                            for sel in desc_selectors:
                                node = s.select_one(sel)
                                if node and node.get_text(strip=True):
                                    found_desc = node.get_text(" ", strip=True)
                                    break
                            if not found_desc:
                                # try JSON-LD @type JobPosting
                                for script in s.find_all("script", type="application/ld+json"):
                                    txt = script.string or ""
                                    try:
                                        payload = json.loads(txt)
                                    except:
                                        try:
                                            payload = json.loads("[" + txt + "]")
                                        except:
                                            payload = None
                                    if payload:
                                        items = payload if isinstance(payload, list) else [payload]
                                        for item in items:
                                            if isinstance(item, dict) and item.get("@type","").lower() == "jobposting":
                                                jd = item.get("description") or item.get("jobLocation") or ""
                                                if isinstance(jd, str) and jd.strip():
                                                    found_desc = jd
                                                    break
                                        if found_desc:
                                            break

                            if not found_desc:
                                # fallback: choose main content body text but prune nav/footer
                                body = s.find("body")
                                if body:
                                    # remove nav/footer
                                    for r in body.select("nav, footer, script, style, noscript, header"):
                                        r.decompose()
                                    found_desc = body.get_text(" ", strip=True)[:20000]  # cap
                            description_text = (found_desc or "").strip()

                            # posting date extraction via many patterns
                            posting_date = ""
                            # 1) JSON-LD datePosted
                            for script in s.find_all("script", type="application/ld+json"):
                                txt = script.string or ""
                                try:
                                    payload = json.loads(txt)
                                except:
                                    try:
                                        payload = json.loads("[" + txt + "]")
                                    except:
                                        payload = None
                                if payload:
                                    items = payload if isinstance(payload, list) else [payload]
                                    for item in items:
                                        if isinstance(item, dict):
                                            dp = item.get("datePosted") or item.get("datePosted")
                                            if isinstance(dp, str) and dp:
                                                posting_date = parse_iso_date_like(dp)
                                                if posting_date:
                                                    break
                                if posting_date:
                                    break

                            # 2) meta tags and specific keys
                            if not posting_date:
                                posting_date = parse_iso_date_like(detail_html)

                            # 3) look for 'Posted on Month day, Year' near title block
                            if not posting_date and h1:
                                combined = h1.get_text(" ", strip=True) + " " + (h1.parent.get_text(" ", strip=True) if h1.parent else "")
                                posting_date = parse_iso_date_like(combined)

                            # 4) sometimes anchor contains "posted X days ago"
                            if not posting_date:
                                posting_date = parse_iso_date_like(anchor_text or "")

                            # location: JSON-LD jobLocation or selectors
                            location_guess = ""
                            # 1) JSON-LD jobLocation
                            try:
                                for script in s.find_all("script", type="application/ld+json"):
                                    txt = script.string or ""
                                    try:
                                        payload = json.loads(txt)
                                    except:
                                        try:
                                            payload = json.loads("[" + txt + "]")
                                        except:
                                            payload = None
                                    if not payload:
                                        continue
                                    items = payload if isinstance(payload, list) else [payload]
                                    for item in items:
                                        if not isinstance(item, dict):
                                            continue
                                        jl = item.get("jobLocation") or item.get("jobLocations") or item.get("jobLocationType")
                                        if jl:
                                            if isinstance(jl, list):
                                                jl = jl[0]
                                            if isinstance(jl, dict):
                                                addr = jl.get("address") or jl
                                                if isinstance(addr, dict):
                                                    locality = addr.get("addressLocality") or addr.get("addressRegion") or addr.get("addressCountry")
                                                    if locality:
                                                        location_guess = locality
                                                        break
                                            elif isinstance(jl, str):
                                                location_guess = jl
                                                break
                                    if location_guess:
                                        break
                            except Exception:
                                pass

                            # 2) selectors and breadcrumbs
                            if not location_guess:
                                for sel in ["span.location", ".job-location", ".location", ".posting-location", ".job_meta_location", ".location--name", ".opening__location", ".jobCard-location"]:
                                    eloc = s.select_one(sel)
                                    if eloc and eloc.get_text(strip=True):
                                        location_guess = eloc.get_text(" ", strip=True)
                                        break
                            # 3) breadcrumb nav
                            if not location_guess:
                                crumbs = s.select("nav a, .breadcrumb a, .breadcrumbs a")
                                for cr in crumbs:
                                    txt = cr.get_text(" ", strip=True)
                                    if re.search(r'\b(remote|usa|united states|uk|germany|france|india|singapore|london|berlin|paris|new york)\b', txt, re.I):
                                        location_guess = txt
                                        break

                            # if description_text empty but page is an article (product/blog) then skip: non-job
                            # check for long product/marketing pages (no role words present)
                            if (not ROLE_WORDS_RE.search(description_text or "") and not ROLE_WORDS_RE.search(title_raw or "")):
                                # it's likely a non-job page (blog/product); skip entry
                                print("   [SKIP NON-JOB PAGE] ", link, title_raw[:80])
                                continue

                    # finalize record
                    title_final = clean_title(title_raw)
                    skills = extract_skills_from_text((description_text or "") + " " + (title_final or ""))
                    seniority = detect_seniority(title_final, description_text)

                    # days since posted
                    pd_final = posting_date or ""
                    days_since = ""
                    if pd_final:
                        try:
                            d = datetime.fromisoformat(pd_final).date()
                            days_since = str((date.today() - d).days)
                        except:
                            days_since = ""

                    rows.append({
                        "Company": company,
                        "Job Title": title_final,
                        "Job Link": link,
                        "Location": (location_guess or "").strip(),
                        "Posting Date": pd_final,
                        "Days Since Posted": days_since,
                        "Function": detect_function(title_final),
                        "Seniority": seniority,
                        "Skills": json.dumps(skills)
                    })

        browser.close()

    # write CSV via pandas if available, else simple CSV
    try:
        import pandas as _pd
        df = _pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["Job Link"])
        df.to_csv(output_csv, index=False)
        print("[OK] wrote", len(df), "rows ->", output_csv)
    except Exception:
        import csv
        keys = rows[0].keys() if rows else []
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            writer.writerows(rows)
        print("[OK] wrote", len(rows), "rows ->", output_csv)

# small function detection re-used in cleaner
def detect_function(title):
    tl = (title or "").lower()
    if re.search(r'\b(data engineer|data scientist|data analyst|machine learning|ml|etl|pipeline)\b', tl):
        return "Data/Analytics"
    if re.search(r'\b(product manager|product|pm|ux|designer)\b', tl):
        return "Product"
    if re.search(r'\b(sales|account|business development|bd|account executive|ae)\b', tl):
        return "Sales"
    if re.search(r'\b(customer success|cs|support|implementation)\b', tl):
        return "Customer Success"
    if re.search(r'\b(marketing|growth|demand)\b', tl):
        return "Marketing"
    if re.search(r'\b(engineer|developer|sre|backend|frontend|devops|software|infrastructure)\b', tl):
        return "Engineering"
    return "Other"

# ---- CLEANER (if you already have CSV) ----
def run_cleaner(infile="jobs_final_hard.csv", outfile="jobs_cleaned_final_enriched.csv"):
    if pd is None:
        raise RuntimeError("pandas required for cleaning. pip install pandas")
    df = pd.read_csv(infile, dtype=str).fillna("")

    # normalize company/title/link
    df["Company"] = df["Company"].apply(lambda x: re.sub(r'\s+', ' ', (x or "")).strip())
    df["Job Title"] = df["Job Title"].apply(lambda x: clean_title(x or ""))
    df["Job Link"] = df["Job Link"].apply(lambda x: (x or "").strip())

    # filter obviously wrong entries (non-job pages)
    def keep_row(r):
        t = (r["Job Title"] or "").lower()
        l = (r["Job Link"] or "").lower()
        if NON_JOB_HINTS.search(t) and not ROLE_WORDS_RE.search(t):
            return False
        if l.startswith("mailto:") or l.startswith("tel:"):
            return False
        # if link domain is product/blog and title not role-like -> drop
        if NON_JOB_HINTS.search(l) and not ROLE_WORDS_RE.search(t) and not any(d in l for d in ATS_DOMAINS):
            return False
        return True
    df = df[df.apply(keep_row, axis=1)].copy()

    # try to fill missing location from title or link
    def extract_location(title, location_cell):
        if location_cell and str(location_cell).strip():
            return normalize_location_cell(location_cell)
        # trailing parentheses e.g. "Senior Engineer (Berlin, Germany)"
        m = re.search(r'\(([^)]+)\)\s*$', title)
        if m:
            return normalize_location_cell(m.group(1))
        # trailing " - Berlin"
        parts = re.split(r'\s+-\s+|\s+\|\s+|\s+—\s+', title)
        if len(parts) > 1:
            cand = parts[-1]
            if len(cand.split()) <= 4 and re.search(r'[A-Za-z]', cand):
                return normalize_location_cell(cand)
        return ""

    df["Location"] = df.apply(lambda r: extract_location(r["Job Title"], r.get("Location","")), axis=1)

    # ensure skills column exists (if present, leave; else try to extract from title)
    if "Skills" not in df.columns:
        df["Skills"] = df["Job Title"].apply(lambda t: json.dumps(extract_skills_from_text(t)))
    else:
        # normalize existing Skills (if stored as JSON list)
        def normalize_skills_cell(s):
            if not s: return "[]"
            try:
                # if already json list string
                lst = json.loads(s)
                if isinstance(lst, list):
                    return json.dumps(list(dict.fromkeys([x.lower() for x in lst])))
            except:
                # try extract from text
                return json.dumps(extract_skills_from_text(str(s)))
            return "[]"
        df["Skills"] = df["Skills"].apply(normalize_skills_cell)

    # detect function & seniority using descriptions if available
    if "Description" in df.columns:
        df["Seniority"] = df.apply(lambda r: detect_seniority(r["Job Title"], r.get("Description","")), axis=1)
    else:
        df["Seniority"] = df["Job Title"].apply(lambda t: detect_seniority(t, ""))

    df["Function"] = df["Job Title"].apply(detect_function)

    # normalize posting date with parse_iso_date_like if possible
    def norm_date(s):
        v = parse_iso_date_like(str(s or ""))
        return v or ""
    if "Posting Date" in df.columns:
        df["Posting Date"] = df["Posting Date"].apply(norm_date)

    # compute Days Since Posted
    def days_since(s):
        try:
            if not s: return ""
            d = datetime.fromisoformat(s).date()
            return str((date.today() - d).days)
        except:
            return ""
    df["Days Since Posted"] = df["Posting Date"].apply(days_since)

    # reorder and save
    out_cols = ["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Function","Seniority","Skills"]
    for c in out_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[out_cols]
    df = df.drop_duplicates(subset=["Job Link"], keep="first").sort_values(by=["Company","Job Title"])
    df.to_csv(outfile, index=False)
    print("[OK] wrote", outfile, "with", len(df), "rows")

# utility used by cleaner
def normalize_location_cell(loc):
    if not loc or not str(loc).strip():
        return ""
    s = re.sub(r'[\n\r\t]', ' ', str(loc))
    parts = [p.strip() for p in re.split(r'[,/;|]+', s) if p.strip()]
    seen = set(); out = []
    for p in parts:
        k = p.lower()
        if k in seen: continue
        seen.add(k)
        if k == "remote":
            out.append("Remote")
        else:
            out.append(p.title())
    return ", ".join(out)

# ---- CLI ----
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scrape", action="store_true", help="Run scraper then write jobs_final_hard.csv")
    ap.add_argument("--clean", action="store_true", help="Run cleaner on jobs_final_hard.csv and write jobs_cleaned_final_enriched.csv")
    args = ap.parse_args()

    if args.scrape:
        run_scraper("jobs_final_hard.csv")
    elif args.clean:
        run_cleaner("jobs_final_hard.csv","jobs_cleaned_final_enriched.csv")
    else:
        print("Nothing requested. Use --scrape or --clean")
        sys.exit(1)
