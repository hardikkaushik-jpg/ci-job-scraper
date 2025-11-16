#!/usr/bin/env python3
# jobs_smart.py
# Final Hard Version – Clean, Strict, Accurate Job Scraper
# OUTPUT: job_final_hard.csv

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
import re, csv, time
from urllib.parse import urljoin, urlparse
from datetime import datetime, date

OUTPUT_FILE = "job_final_hard.csv"

# --- COMPANY LIST (FULL 56) ---
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
        "https://www.mongodb.com/company/careers/teams/product-management-and-design",
    ],
    "Monte Carlo": ["https://jobs.ashbyhq.com/montecarlodata"],
    "Mulesoft": ["https://www.mulesoft.com/careers"],
    "Nutanix": ["https://careers.nutanix.com/en/jobs/"],
    "OneTrust": ["https://www.onetrust.com/careers/"],
    "Oracle": ["https://careers.oracle.com/en/sites/jobsearch/jobs"],
    "Panoply (Sqream)": ["https://sqream.com/careers/"],
    "Precisely": [
        "https://www.precisely.com/careers-and-culture/us-jobs",
        "https://www.precisely.com/careers-and-culture/international-jobs",
    ],
    "Qlik": ["http://careerhub.qlik.com/careers"],
    "Sifflet": ["https://www.welcometothejungle.com/en/companies/sifflet/jobs"],
    "SnapLogic": ["https://www.snaplogic.com/company/careers/job-listings"],
    "Snowflake": ["https://careers.snowflake.com/us/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"],
}

# Cleaning rules
LANG_RE = re.compile(r'\b(Deutsch|Français|Italiano|日本語|Português|Español|English|한국어|简体中文)\b', re.I)
REMOVE_PHRASES = re.compile(
    r"(learn more.*|apply.*|view job.*|view role.*|careers|job listings|open roles|positions|dashboard|profile|policy)",
    re.I,
)
LOCATION_RE = re.compile(
    r"\b(?:Remote|Hybrid|USA|India|Germany|France|Canada|Spain|UK|United Kingdom|United States|Bengaluru|Berlin|Paris|London|New York|Atlanta|Georgia)\b",
    re.I,
)

JOB_HINTS = [
    "greenhouse.io",
    "lever.co",
    "ashbyhq",
    "myworkdayjobs",
    "bamboohr",
    "/job/",
    "/jobs/",
    "/careers/",
    "apply",
]

def clean_title(t):
    if not t:
        return ""
    t = LANG_RE.sub("", t)
    t = REMOVE_PHRASES.sub("", t)
    t = LOCATION_RE.sub("", t)
    t = re.sub(r"\s{2,}", " ", t).strip(" -:\n\t")
    return t

def extract_location(txt):
    m = LOCATION_RE.search(txt)
    return m.group(0) if m else ""

def extract_posting_date(html):
    soup = BeautifulSoup(html, "html.parser")
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        try:
            return time_tag["datetime"][:10]
        except:
            pass
    match = re.search(r'"datePosted":"(.*?)"', html)
    if match:
        return match.group(1)[:10]
    return ""

def days_since(date_str):
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return (date.today() - d).days
    except:
        return ""

def is_job_link(href):
    if not href: return False
    h = href.lower()
    return any(k in h for k in JOB_HINTS)

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()

        for company, urls in COMPANIES.items():
            for url in urls:
                print(f"[SCRAPING] {company} → {url}")

                try:
                    page.goto(url, timeout=45000)
                    page.wait_for_load_state("networkidle", timeout=15000)
                except:
                    continue

                anchors = page.query_selector_all("a[href]")
                for a in anchors:
                    href = a.get_attribute("href")
                    text = (a.inner_text() or "").strip()

                    if not href: 
                        continue

                    full = urljoin(url, href)

                    if is_job_link(full):
                        title_clean = clean_title(text)
                        if not title_clean:
                            continue

                        # open job detail page
                        try:
                            page.goto(full, timeout=35000)
                            page.wait_for_load_state("networkidle", timeout=12000)
                            html = page.content()
                        except:
                            html = ""

                        loc = extract_location(text + " " + html)
                        posted = extract_posting_date(html)
                        days = days_since(posted)

                        rows.append({
                            "Company": company,
                            "Job Title": title_clean,
                            "Job Link": full,
                            "Location": loc,
                            "Posting Date": posted,
                            "Days Since Posted": days
                        })
                time.sleep(0.2)

        browser.close()

    # write file
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"]
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"[OK] Saved {len(rows)} jobs → {OUTPUT_FILE}")

if __name__ == "__main__":
    scrape()
