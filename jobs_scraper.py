#!/usr/bin/env python3
# jobs_scraper.py (tight output)
# Updated scraper: site-specific extraction + stricter "tight" filtering -> writes jobs_tight.csv

import requests, json, re, sys, os
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
from datetime import datetime

# -------------------- CONFIG: paste your 56 entries (I included the list you provided) --------------------
COMPANIES = {
    "Airtable": "https://airtable.com/careers",
    "Alation": "https://www.alation.com/careers/all-careers/",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://www.anomalo.com/careers",
    "BigEye": "https://www.bigeye.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://coalesce.io/careers/",
    "Cloudera": "https://www.cloudera.com/careers",
    "Collibra": "https://www.collibra.com/company/careers",
    "Couchbase": "https://www.couchbase.com/careers/",
    "Data.World (ServiceNow)": "https://data.world/company/careers",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://www.datadoghq.com/careers/open-roles/",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube": "https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol": "https://careers.exasol.com/",
    "Firebolt": "https://www.firebolt.io/careers",
    "Fivetran": "https://fivetran.com/careers",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "IBM": "https://www.ibm.com/careers/us-en/search/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs?utm_medium=Direct",
    "MariaDB": "https://mariadb.com/about/careers/",
    "Matillion": "https://www.matillion.com/careers",
    "MongoDB (Engineering)": "https://www.mongodb.com/company/careers/teams/engineering",
    "MongoDB (Marketing)": "https://www.mongodb.com/company/careers/teams/marketing",
    "MongoDB (Sales)": "https://www.mongodb.com/company/careers/teams/sales",
    "MongoDB (Product)": "https://www.mongodb.com/company/careers/teams/product-management-and-design",
    "Monte Carlo": "https://jobs.ashbyhq.com/montecarlodata",
    "Mulesoft": "https://www.mulesoft.com/careers",
    "Nutanix": "https://careers.nutanix.com/en/jobs/",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Oracle": "https://careers.oracle.com/en/sites/jobsearch/jobs?mode=location",
    "Panoply": "https://sqream.com/careers/",
    "PostgreSQL": "https://www.postgresql.org/about/careers/",
    "Precisely (US)": "https://www.precisely.com/careers-and-culture/us-jobs",
    "Precisely (Int)": "https://www.precisely.com/careers-and-culture/international-jobs",
    "Qlik": "https://careerhub.qlik.com/careers?start=0&pid=1133909999056&sort_by=hot",
    "SAP": "https://www.sap.com/about/careers.html",
    "Sifflet": "https://www.welcometothejungle.com/en/companies/sifflet/jobs",
    "SnapLogic": "https://www.snaplogic.com/company/careers",
    "Snowflake": "https://careers.snowflake.com/",
    "Solidatus": "https://www.solidatus.com/careers/",
    "SQLite": "https://www.sqlite.org/careers.html",
    "Syniti": "https://careers.syniti.com/",
    "Tencent Cloud": "https://careers.tencent.com/en-us/search.html",
    "Teradata": "https://careers.teradata.com/jobs",
    "Yellowbrick": "https://yellowbrick.com/careers/#positions",
    "Vertica": "https://careers.opentext.com/us/en",
    "Pentaho": "https://www.hitachivantara.com/en-us/company/careers/job-search"
}

# -------------------- SETTINGS --------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; job-scraper/1.0; +mailto:you@example.com)"}
TIMEOUT = 15
OUT_FILE = "jobs_tight.csv"   # output file name (change if you want)
# ------------------------------------------------------------------------

# Regex patterns used for heuristics
JOB_LINK_RE = re.compile(r'(/jobs/|/careers|jobs\.ashbyhq\.com|jobs\.lever\.co|greenhouse\.io|boards\.greenhouse|myworkdayjobs|wd\d+\.myworkdayjobs|bamboohr|comeet|boards\.briohr|job-boards|/job/|careers\.)', re.I)
NOISE_LINK_RE = re.compile(r'^(mailto:|tel:|javascript:)|linkedin\.com/share|/privacy|/terms|/blog|/press|/about|/contact|facebook\.com|twitter\.com|instagram\.com', re.I)
LANG_ONLY_RE = re.compile(r'^(english|deutsch|français|italiano|日本語|español|portugu[eé]s|german|french|italian|spanish)$', re.I)
GENERIC_TITLE_RE = re.compile(r'^(careers?$|view all positions|learn more|open roles|open jobs|see careers|find your next role|culture|benefits|internships|jobs ?\d*$)', re.I)
ROLE_KW_RE = re.compile(r'\b(engineer|developer|manager|analyst|product|data|scientist|director|lead|principal|intern|consultant|account executive|solutions engineer|sre|qa|tester|ux|ui|designer|marketing|sales|revenue|operations|legal|counsel)\b', re.I)
APPLY_REGION_RE = re.compile(r'\bapply\b', re.I)

# -------------------- helper functions --------------------
def fetch_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[WARN] fetch failed for {url}: {e}")
        return ""

def parse_json_ld(html, base_url):
    out = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "{}")
            except:
                continue
            # single dict
            if isinstance(data, dict):
                if data.get("@type","").lower() == "jobposting":
                    out.append({"title": data.get("title",""), "link": data.get("url") or base_url})
                # sometimes JobPosting appears inside list in dict under @graph
                if "@graph" in data and isinstance(data["@graph"], list):
                    for item in data["@graph"]:
                        if isinstance(item, dict) and item.get("@type","").lower()=="jobposting":
                            out.append({"title": item.get("title",""), "link": item.get("url") or base_url})
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type","").lower()=="jobposting":
                        out.append({"title": item.get("title",""), "link": item.get("url") or base_url})
    except Exception as e:
        pass
    return out

# Site-specific small extractors
def extract_lever(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if "/jobs/" in href or "jobs.lever.co" in href:
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": a.get_text(" ", strip=True), "link": full})
    return rows

def extract_greenhouse(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.select("a[href*='/jobs/']"):
        href = a["href"]
        full = href if urlparse(href).netloc else urljoin(base_url, href)
        rows.append({"title": a.get_text(" ", strip=True), "link": full})
    return rows

def extract_workday(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if "workday" in href.lower() or "/job/" in href.lower() or "myworkdayjobs" in href.lower():
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": a.get_text(" ", strip=True), "link": full})
    return rows

def extract_ashby(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if "ashby" in href or "/jobs/" in href:
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": a.get_text(" ", strip=True), "link": full})
    return rows

def extract_generic(base_url, html):
    # parse JSON-LD first (best quality)
    out = parse_json_ld(html, base_url)
    if out:
        return out
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        # skip obvious noise
        if NOISE_LINK_RE.search(href) or href.lower().startswith("mailto:") or href.lower().startswith("tel:"):
            continue
        if JOB_LINK_RE.search(href) or JOB_LINK_RE.search(text) or ROLE_KW_RE.search(text):
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": text, "link": full})
    return rows

def normalize_title(title):
    t = (title or "").strip()
    # drop language prefixes
    t = re.sub(r'^(english|deutsch|français|italiano|日本語|español|portugu[eé]s)[:,]?\s*', '', t, flags=re.I)
    # remove leading "Careers -", "View Openings -", "Apply in"
    t = re.sub(r'^(careers?[:\-]?\s*)', '', t, flags=re.I)
    t = re.sub(r'^(view open(ings|ings)?[:\-]?\s*)', '', t, flags=re.I)
    t = re.sub(r'^\bapply\b(\s+in)?\b[:\-]?\s*', '', t, flags=re.I)
    t = re.sub(r'\s{2,}', ' ', t).strip(" ,:-")
    return t

def looks_like_job(title, link):
    if not link:
        return False
    link_l = link.lower()
    title_l = (title or "").lower()
    # reject mailto/tel
    if link_l.startswith("mailto:") or link_l.startswith("tel:"):
        return False
    # reject social/press/privacy
    if re.search(r'linkedin\.com/share|/privacy|/terms|/blog|/press|/about|/contact|facebook\.com|twitter\.com|instagram\.com', link_l):
        return False
    # if link pattern indicates job platform -> accept
    if JOB_LINK_RE.search(link_l):
        # reject generic career landing with no role in title
        path = urlparse(link_l).path or ""
        if re.search(r'/careers/?$|/careers#?$', path) and not ROLE_KW_RE.search(title):
            return False
        return True
    # otherwise accept only if title contains role keyword
    if ROLE_KW_RE.search(title):
        return True
    return False

def guess_posting_date_from_detail(link):
    html = fetch_html(link)
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    if m:
        return m.group(1)
    m = re.search(r'([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})', text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y").date().isoformat()
        except:
            try:
                return datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
            except:
                return m.group(1)
    return ""

# -------------------- Scrape flow --------------------
def scrape_company(name, url):
    print(f"[INFO] Scraping {name} -> {url}")
    html = fetch_html(url)
    if not html:
        print(f"[WARN] Empty html for {url}")
        return []
    # choose extractor by known host
    host = urlparse(url).netloc.lower()
    candidates = []
    try:
        if "lever.co" in url or "jobs.lever.co" in url:
            candidates = extract_lever(url, html)
        elif "greenhouse" in url:
            candidates = extract_greenhouse(url, html)
        elif "myworkdayjobs" in url or "workday" in url or "/wd" in url:
            candidates = extract_workday(url, html)
        elif "ashby" in url:
            candidates = extract_ashby(url, html)
        else:
            candidates = extract_generic(url, html)
    except Exception as e:
        print(f"[WARN] extractor failed for {url}: {e}")
        candidates = extract_generic(url, html)

    results = []
    seen = set()
    for c in candidates:
        title = normalize_title(c.get("title","") or "")
        link = c.get("link") or url
        # normalize link (remove tracking query params)
        parsed = urlparse(link)
        link_clean = parsed.scheme + "://" + parsed.netloc + parsed.path
        # final heuristic accept/reject
        if not looks_like_job(title, link_clean):
            continue
        # dedupe
        key = (name, link_clean)
        if key in seen:
            continue
        seen.add(key)
        # try get posting date (best-effort, optional)
        posted = ""
        try:
            posted = guess_posting_date_from_detail(link_clean)
        except Exception:
            posted = ""
        results.append({
            "Company": name,
            "Job Title": title or "(no title)",
            "Job Link": link_clean,
            "Location": "",
            "Posting Date": posted
        })
    print(f"[INFO] {name}: {len(results)} job rows accepted")
    return results

def main():
    all_rows = []
    for name, url in COMPANIES.items():
        try:
            rows = scrape_company(name, url)
            all_rows.extend(rows)
        except Exception as e:
            print(f"[ERROR] scraping {name}: {e}")

    if not all_rows:
        print("[WARN] no job rows collected - exiting without writing file")
        return

    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["Company","Job Link"], inplace=True)
    # Write into repo root (or working directory)
    out_path = os.path.join(os.getcwd(), OUT_FILE)
    df.to_csv(out_path, index=False)
    print(f"[OK] Wrote {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()
