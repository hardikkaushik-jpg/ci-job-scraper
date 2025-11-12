#!/usr/bin/env python3
"""
jobs_scraper.py -> writes jobs_tighter.csv

Designed to:
 - Prefer ATS job feeds (Greenhouse/Lever/Workday/Ashby/BambooHR/SmartRecruiters/Comeet/BrioHR)
 - Use JSON-LD JobPosting when present
 - Avoid product/solutions/resource pages (so Boomi/Databricks/Collibra should give real job posts)
 - Best-effort extraction of posting date (JSON-LD -> detail page date parsing)
"""

import requests, re, json, time, csv, os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime

# ---------- CONFIG ----------
OUT_FILE = "jobs_tighter.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (job-scraper; +mailto:you@example.com)"}
TIMEOUT = 15
POLITENESS = 0.7

# ---------- Full Companies list (from your provided list) ----------
COMPANIES = {
    "Airtable":"https://airtable.com/careers",
    "Alation":"https://www.alation.com/careers/all-careers/",
    "Alex Solutions":"https://alexsolutions.com/careers",
    "Alteryx":"https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Amazon (AWS)":"https://www.amazon.jobs/en/teams/aws",
    "Ataccama":"https://jobs.ataccama.com/",
    "Atlan":"https://atlan.com/careers",
    "Anomalo":"https://www.anomalo.com/careers",
    "BigEye":"https://www.bigeye.com/careers",
    "Boomi":"https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)":"https://coalesce.io/company/careers",
    "Cloudera":"https://www.cloudera.com/careers",
    "Collibra":"https://www.collibra.com/company/careers",
    "Couchbase":"https://www.couchbase.com/careers/",
    "Data.World (ServiceNow)":"https://data.world/company/careers",
    "Databricks":"https://databricks.com/company/careers/open-positions",
    "Datadog":"https://www.datadoghq.com/careers/open-roles/",
    "DataGalaxy":"https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube":"https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol":"https://careers.exasol.com/",
    "Firebolt":"https://www.firebolt.io/careers",
    "Fivetran":"https://fivetran.com/careers",
    "GoldenSource":"https://www.thegoldensource.com/careers/",
    "Google (General)":"https://careers.google.com/jobs/results/",
    "IBM":"https://www.ibm.com/careers/us-en/search/",
    "InfluxData":"https://www.influxdata.com/careers/",
    "Informatica":"https://informatica.gr8people.com/jobs?utm_medium=Direct",
    "MariaDB":"https://mariadb.com/about/careers/",
    "Matillion":"https://www.matillion.com/careers",
    "Microsoft":"https://careers.microsoft.com/us/en/search-results",
    "MongoDB (Engineering)":"https://www.mongodb.com/company/careers/teams/engineering",
    "MongoDB (Marketing)":"https://www.mongodb.com/company/careers/teams/marketing",
    "MongoDB (Sales)":"https://www.mongodb.com/company/careers/teams/sales",
    "MongoDB (Product)":"https://www.mongodb.com/company/careers/teams/product-management-and-design",
    "Monte Carlo":"https://jobs.ashbyhq.com/montecarlodata",
    "Mulesoft":"https://www.mulesoft.com/careers",
    "Nutanix":"https://careers.nutanix.com/en/jobs/",
    "OneTrust":"https://www.onetrust.com/careers/",
    "Oracle":"https://careers.oracle.com/en/sites/jobsearch/jobs?mode=location",
    "Panoply":"https://sqream.com/careers/",
    "PostgreSQL":"https://www.postgresql.org/about/careers/",
    "Precisely (US)":"https://www.precisely.com/careers-and-culture/us-jobs",
    "Precisely (Int)":"https://www.precisely.com/careers-and-culture/international-jobs",
    "Qlik":"https://careerhub.qlik.com/careers?start=0&pid=1133909999056&sort_by=hot",
    "SAP":"https://www.sap.com/about/careers.html",
    "Sifflet":"https://www.welcometothejungle.com/en/companies/sifflet/jobs",
    "SnapLogic":"https://www.snaplogic.com/company/careers",
    "Snowflake":"https://careers.snowflake.com/",
    "Solidatus":"https://www.solidatus.com/careers/",
    "SQLite":"https://www.sqlite.org/careers.html",
    "Syniti":"https://careers.syniti.com/",
    "Tencent Cloud":"https://careers.tencent.com/en-us/search.html",
    "Teradata":"https://careers.teradata.com/jobs",
    "Yellowbrick":"https://yellowbrick.com/careers/#positions",
    "Vertica":"https://careers.opentext.com/us/en",
    "Pentaho":"https://www.hitachivantara.com/en-us/company/careers/job-search",
}

# ---------- ATS host tokens to look for on pages ----------
ATS_HOST_TOKENS = [
    "boards.greenhouse.io", "jobs.lever.co", "myworkdayjobs.com", "workday.com",
    "jobs.ashbyhq.com", "bamboohr.com", "smartrecruiters.com", "comeet.co", "boards.briohr.com",
]

# ---------- regex / helpers ----------
JOB_URL_RE = re.compile(r'(/job/|/jobs/|jobs\.|/careers/jobs/|/careers/positions/|/open-roles|/open-positions|/careers/|/careers\?)', re.I)
ROLE_KW_RE = re.compile(r'\b(engineer|developer|data|analyst|manager|architect|devops|sre|qa|quality|product|designer|ux|ui|sales|solutions engineer|account executive|consultant|scientist|director|lead|researcher)\b', re.I)
DATE_PATTERNS = [
    re.compile(r'(\d{4}-\d{2}-\d{2})'),
    re.compile(r'([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})'),
    re.compile(r'(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})'),
]

DENY_PATH_TOKENS = ["/product", "/products", "/solutions", "/resources", "/blog", "/docs", "/documentation", "/webinar", "/events", "/pricing", "/case-study", "/case-studies"]

def fetch_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[WARN] fetch_html failed for {url}: {e}")
        return ""

def normalize_link(link, base=None):
    if not link:
        return ""
    link = link.strip()
    if link.startswith("//"):
        link = "https:" + link
    parsed = urlparse(link)
    if not parsed.scheme:
        if base:
            return urljoin(base, link)
        else:
            return "https://" + link
    # remove query & fragment
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def find_ats_links(html, base_url):
    """Scan anchors for known ATS host links and return them (unique)."""
    out = []
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = normalize_link(href, base_url)
        for token in ATS_HOST_TOKENS:
            if token in full:
                if full not in out:
                    out.append(full)
    return out

def parse_jsonld_jobposting(html, base_url):
    """Return list of job dicts from JSON-LD JobPosting"""
    out = []
    try:
        soup = BeautifulSoup(html, "lxml")
        scripts = soup.find_all("script", {"type":"application/ld+json"})
        for s in scripts:
            raw = s.string
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except:
                # skip broken JSON-LD
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                # handle @graph
                if isinstance(item, dict) and "@graph" in item and isinstance(item["@graph"], list):
                    items2 = item["@graph"]
                else:
                    items2 = [item]
                for it in items2:
                    if isinstance(it, dict):
                        typ = it.get("@type") or it.get("type") or ""
                        if isinstance(typ, list):
                            typ = ",".join(typ)
                        if "JobPosting" in str(typ):
                            title = it.get("title") or it.get("name") or ""
                            link = it.get("url") or base_url
                            datep = it.get("datePosted") or ""
                            loc = ""
                            jl = it.get("jobLocation")
                            if isinstance(jl, dict):
                                addr = jl.get("address", {})
                                loc = addr.get("addressLocality","") or addr.get("addressRegion","") or ""
                            out.append({"title": title.strip(), "link": link.strip(), "location": loc, "datePosted": datep})
    except Exception as e:
        print(f"[WARN] parse_jsonld_jobposting error: {e}")
    return out

def find_job_anchors_conservative(html, base_url):
    """Conservative anchor scanning: only anchors that look like jobs and are not product pages"""
    out = []
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True) or ""
        full = normalize_link(href, base_url)
        if not full:
            continue
        path = urlparse(full).path.lower()
        if any(tok in path for tok in DENY_PATH_TOKENS):
            continue
        if JOB_URL_RE.search(full) or ROLE_KW_RE.search(text):
            out.append({"title": text.strip(), "link": full})
    # dedupe by link
    uniq = []
    seen = set()
    for c in out:
        if c["link"] not in seen:
            seen.add(c["link"])
            uniq.append(c)
    return uniq

def guess_date_from_detail(detail_html):
    if not detail_html:
        return ""
    text = BeautifulSoup(detail_html, "lxml").get_text(" ", strip=True)
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            ds = m.group(1)
            for fmt in ("%Y-%m-%d","%B %d, %Y","%b %d, %Y","%d %B %Y"):
                try:
                    dt = datetime.strptime(ds, fmt)
                    return dt.date().isoformat()
                except:
                    pass
            return ds
    return ""

def follow_and_extract(company, careers_url):
    """
    Strategy:
     1) fetch careers page
     2) try JSON-LD on that page
     3) find ATS links on that page and follow them (prefer first)
     4) else conservative anchor extraction
    """
    out = []
    careers_html = fetch_html(careers_url)
    if not careers_html:
        return out

    # 1) JSON-LD on careers page
    jl = parse_jsonld_jobposting(careers_html, careers_url)
    if jl:
        for j in jl:
            title = j.get("title") or ""
            link = normalize_link(j.get("link") or careers_url, careers_url)
            datep = j.get("datePosted") or ""
            loc = j.get("location") or ""
            # final acceptance test
            if ROLE_KW_RE.search(title) or JOB_URL_RE.search(link):
                # try to fill date if missing
                if not datep:
                    detail_html = fetch_html(link)
                    datep = guess_date_from_detail(detail_html)
                out.append({"Company": company, "Job Title": title, "Job Link": link, "Location": loc, "Posting Date": datep})
        if out:
            return out

    # 2) find ATS links embedded and follow them (strong preference)
    ats_links = find_ats_links(careers_html, careers_url)
    for ats in ats_links:
        # fetch ATS page (list) and try JSON-LD or anchors there
        ats_html = fetch_html(ats)
        if not ats_html:
            continue
        # JSON-LD on ATS page
        jl2 = parse_jsonld_jobposting(ats_html, ats)
        if jl2:
            for j in jl2:
                title = j.get("title") or ""
                link = normalize_link(j.get("link") or ats, ats)
                datep = j.get("datePosted") or ""
                loc = j.get("location") or ""
                if not datep:
                    detail_html = fetch_html(link)
                    datep = guess_date_from_detail(detail_html)
                if ROLE_KW_RE.search(title) or JOB_URL_RE.search(link):
                    out.append({"Company": company, "Job Title": title, "Job Link": link, "Location": loc, "Posting Date": datep})
        # fallback: conservative anchors on ATS page
        candidates = find_job_anchors_conservative(ats_html, ats)
        for c in candidates:
            title = c.get("title") or ""
            link = normalize_link(c.get("link") or ats, ats)
            detail_html = fetch_html(link)
            datep = guess_date_from_detail(detail_html)
            if ROLE_KW_RE.search(title) or JOB_URL_RE.search(link):
                out.append({"Company": company, "Job Title": title, "Job Link": link, "Location": "", "Posting Date": datep})
        if out:
            return out

    # 3) fallback: conservative anchors on careers page itself
    candidates = find_job_anchors_conservative(careers_html, careers_url)
    for c in candidates:
        title = c.get("title") or ""
        link = normalize_link(c.get("link") or careers_url, careers_url)
        detail_html = fetch_html(link)
        datep = guess_date_from_detail(detail_html)
        if ROLE_KW_RE.search(title) or JOB_URL_RE.search(link):
            out.append({"Company": company, "Job Title": title, "Job Link": link, "Location": "", "Posting Date": datep})

    return out

def run_all():
    all_rows = []
    for name, url in COMPANIES.items():
        try:
            rows = follow_and_extract(name, url)
            # small delay
            time.sleep(POLITENESS)
            # dedupe inside company by link
            seen = set()
            for r in rows:
                lk = r.get("Job Link","")
                if not lk: continue
                if lk in seen: continue
                seen.add(lk)
                all_rows.append(r)
            print(f"[INFO] {name} -> extracted {len(seen)} jobs")
        except Exception as e:
            print(f"[ERROR] {name} failed: {e}")

    # final dedupe overall
    uniq = {}
    for r in all_rows:
        key = (r.get("Company",""), r.get("Job Link",""))
        if key not in uniq:
            uniq[key] = r

    out = list(uniq.values())
    # write CSV
    fieldnames = ["Company","Job Title","Job Link","Location","Posting Date"]
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in out:
            writer.writerow({k: r.get(k,"") for k in fieldnames})
    print(f"[OK] Wrote {len(out)} rows to {OUT_FILE}")

if __name__ == "__main__":
    run_all()
