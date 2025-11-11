# jobs_scraper.py
# Simple multi-site job-scraper prototype for GitHub Actions
# It reads a list of career URLs from the COMPANIES list and scrapes job links/titles/dates.
# Output: jobs.csv in repo root (committed by the workflow).

import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime

# --- CONFIG: Competitor career URLs ---
COMPANIES = {
    "Airtable": "https://airtable.com/careers",
    "Alation": "https://www.alation.com/careers/all-careers/",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Amazon (AWS)": "https://www.amazon.jobs/en/teams/aws",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://www.anomalo.com/careers",
    "BigEye": "https://www.bigeye.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://coalesce.io/company/careers",
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
    "Google (General)": "https://careers.google.com/jobs/results/",
    "IBM": "https://www.ibm.com/careers/us-en/search/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs?utm_medium=Direct",
    "MariaDB": "https://mariadb.com/about/careers/",
    "Matillion": "https://www.matillion.com/careers",
    "Microsoft": "https://careers.microsoft.com/us/en/search-results",
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

# Convenience patterns (job keywords)
JOB_KEYWORDS = ["job", "career", "openings", "positions", "vacancy", "/jobs/", "/careers/"]

def fetch_html(url, headers=None, timeout=15):
    try:
        r = requests.get(url, headers=headers or {"User-Agent":"Mozilla/5.0"}, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[WARN] fetch failed for {url}: {e}")
        return ""

def extract_jobs_generic(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    rows = []
    for a in anchors:
        href = a["href"].strip()
        text = a.get_text(separator=" ", strip=True)
        if not text:
            text = a.get("aria-label","").strip()
        combined = (text + " " + href).lower()
        if any(k in combined for k in ["job", "career", "position", "/jobs", "/careers", "open role", "open-role", "vacancy"]):
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": text[:200], "link": full})
    seen = {}
    out = []
    for r in rows:
        if r["link"] not in seen:
            seen[r["link"]] = True
            out.append(r)
    return out

def try_extract_job_date(soup_el):
    text = soup_el.get_text(" ", strip=True)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", text)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%B %d, %Y")
            return dt.date().isoformat()
        except:
            try:
                dt = datetime.strptime(m.group(1), "%b %d, %Y")
                return dt.date().isoformat()
            except:
                return m.group(1)
    return ""

def scrape_one_company(name, url):
    html = fetch_html(url)
    if not html:
        return []
    candidates = extract_jobs_generic(url, html)
    results = []
    soup = BeautifulSoup(html, "lxml")
    for c in candidates:
        title = c.get("title","").strip()
        link = c.get("link","")
        posted_date = ""
        try:
            detail_html = fetch_html(link)
            if detail_html:
                ds = BeautifulSoup(detail_html, "lxml")
                posted_date = try_extract_job_date(ds) or ""
        except:
            posted_date = ""
        results.append({
            "Company": name,
            "Job Title": title or "(no title)",
            "Job Link": link,
            "Location": "",
            "Posting Date": posted_date
        })
    return results

def main():
    all_rows = []
    for name, url in COMPANIES.items():
        print(f"[INFO] Scraping {name} -> {url}")
        rows = scrape_one_company(name, url)
        if not rows:
            print(f"[WARN] no rows found for {name}. Consider adding a direct Lever/Greenhouse URL for that company.")
        all_rows.extend(rows)
    if not all_rows:
        print("[ERROR] no job rows extracted. Exiting with no CSV.")
        return
    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["Company","Job Link"], inplace=True)
    df = df.sort_values(["Company","Posting Date"], ascending=[True, False])
    out = "jobs.csv"
    df.to_csv(out, index=False)
    print(f"[OK] Wrote {len(df)} rows to {out}")

if __name__ == "__main__":
    main()
