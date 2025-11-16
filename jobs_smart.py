# jobs_smart.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages (Option B)
# Output -> jobs_final_hard.csv

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys
from datetime import datetime, date, timedelta


# ------------------ CONFIG: canonical URLs (ONLY your links) ------------------
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
    "Snowflake": ["https://careers.snowflake.com/global/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"],
}
# ----------------------------------------------------------------------------


PAGE_NAV_TIMEOUT = 45000
PAGE_IDLE_TIMEOUT = 30000
SLEEP = 0.25


IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch","Français","Italiano","日本語","Português","Español","English","한국어","简体中文"]
LANG_RE = re.compile(r"\b(?:%s)\b" % "|".join(LANG_TAGS), re.I)
BAD_LINK = re.compile(r"(product|features|pricing|docs|resources|legal|contact|privacy|cookie)", re.I)


def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)


def clean_title(t):
    if not t:
        return ""
    t = t.replace("\n", " ")
    t = re.sub(r"\s+", " ", t).strip()
    t = LANG_RE.sub("", t)
    t = re.sub(r"learn more.*$", "", t, flags=re.I)
    t = re.sub(r"apply.*$", "", t, flags=re.I)
    t = re.sub(r"\blocation\b|\blocations\b|view openings|careers|career", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -:,.")
    return t


def extract_date(html):
    if not html:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1).split("T")[0]
    m = re.search(r"posted\s+(\d+)\s+days?\s+ago", html, re.I)
    if m:
        return (date.today() - timedelta(days=int(m.group(1)))).isoformat()
    return ""


def days_since(d):
    if not d:
        return ""
    try:
        d0 = datetime.fromisoformat(d).date()
        return str((date.today() - d0).days)
    except:
        return ""


def is_job_link(href, text):
    if not href:
        return False
    h = href.lower()
    t = text.lower()
    if BAD_LINK.search(h) or BAD_LINK.search(t):
        return False
    if any(x in h for x in ["job", "jobs", "position", "open", "apply", "greenhouse", "lever", "workday", "bamboohr", "ashby"]):
        return True
    if any(x in t for x in ["job", "jobs", "position", "openings", "apply", "hiring", "role"]):
        return True
    return False


def parse_listing(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.find_all("a", href=True):
        href = normalize_link(base_url, a["href"])
        text = a.get_text(" ", strip=True)
        if is_job_link(href, text):
            out.append((href, text))
    uniq = []
    seen = set()
    for x in out:
        if x[0] not in seen:
            uniq.append(x)
            seen.add(x[0])
    return uniq


def fetch(page, url):
    try:
        page.goto(url, timeout=PAGE_NAV_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=PAGE_IDLE_TIMEOUT)
        return page.content()
    except:
        return ""


def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context()
        page = ctx.new_page()

        for company, url_list in COMPANIES.items():
            for url in url_list:
                print(f"[SCRAPING] {company} → {url}")

                html = fetch(page, url)
                if not html:
                    continue

                candidates = parse_listing(url, html)

                for link, text in candidates:
                    time.sleep(SLEEP)
                    detail = fetch(page, link)

                    job_title = clean_title(text)
                    job_location = ""
                    posting_date = extract_date(detail)

                    if detail:
                        soup = BeautifulSoup(detail, "lxml")
                        h1 = soup.find("h1")
                        if h1 and len(job_title) < 3:
                            job_title = clean_title(h1.get_text(" ", strip=True))

                        loc = soup.select_one(".location, .job-location, span.location")
                        if loc:
                            job_location = loc.get_text(" ", strip=True)

                    rows.append({
                        "Company": company,
                        "Job Title": job_title,
                        "Job Link": link,
                        "Location": job_location,
                        "Posting Date": posting_date,
                        "Days Since Posted": days_since(posting_date)
                    })

        browser.close()

    # Deduplicate
    out = []
    seen = set()
    for r in rows:
        if r["Job Link"] not in seen:
            seen.add(r["Job Link"])
            out.append(r)

    # Write CSV
    with open("jobs_final_hard.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f,
                           fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"])
        w.writeheader()
        for x in out:
            w.writerow(x)

    print(f"[OK] wrote {len(out)} rows → jobs_final_hard.csv")


if __name__ == "__main__":
    scrape()
