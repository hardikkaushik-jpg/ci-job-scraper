# jobs_playwright.py
# Playwright-based ATS-focused job scraper -> produces jobs_tighter.csv
#
# Requirements:
#   pip install -r requirements.txt
#   playwright install
#
# Behavior:
#   - Uses Playwright for JS-heavy pages
#   - Cleans titles, skips non-job pages, and extracts posting date + location
#   - Writes jobs_tighter.csv (overwrites each run)

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import re, csv, time
from urllib.parse import urljoin, urlparse
from datetime import datetime

# --- Helper: Safe navigation wrapper for Playwright ---
def safe_goto(page, url, timeout=60000):
    """Safely navigate to a URL with retries and error handling."""
    try:
        page.on("download", lambda d: print(f"[WARN] download triggered on {url}, skipping"))
        page.goto(url, timeout=timeout, wait_until="domcontentloaded")
        return True
    except Exception as e:
        print(f"[WARN] failed to load {url} ({e}) — skipping")
        return False


# --- CONFIG: Company career URLs ---
COMPANIES = {
    "Airtable": "https://airtable.com/careers",
    "Alation": "https://www.alation.com/careers/all-careers/",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Amazon (AWS)": "https://www.amazon.jobs/en/teams/aws",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://www.anomalo.com/careers",
    "BigEye": "https://www.bigeye.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://coalesce.io/careers/",
    "Cloudera": "https://www.cloudera.com/careers.html",
    "Collibra": "https://www.collibra.com/company/careers",
    "Couchbase": "https://www.couchbase.com/careers/",
    "Data.World (ServiceNow)": "https://data.world/company/careers",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://careers.datadoghq.com/",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube": "https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol": "https://careers.exasol.com/",
    "Firebolt": "https://www.firebolt.io/careers",
    "Fivetran": "https://fivetran.com/careers",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs",
    "MariaDB": "https://mariadb.com/about/careers/",
    "Matillion": "https://www.matillion.com/careers",
    "MongoDB (Engineering)": "https://www.mongodb.com/company/careers/teams/engineering",
    "Monte Carlo": "https://jobs.ashbyhq.com/montecarlodata",
    "Mulesoft": "https://www.mulesoft.com/careers",
    "Nutanix": "https://careers.nutanix.com/en/jobs/",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Oracle": "https://careers.oracle.com/en/sites/jobsearch/jobs",
    "Panoply": "https://sqream.com/careers/",
    "PostgreSQL": "https://www.postgresql.org/about/careers/",
    "Precisely (US)": "https://www.precisely.com/careers-and-culture/us-jobs",
    "Qlik": "http://careerhub.qlik.com/careers",
    "SAP": "https://jobs.sap.com/",
    "Sifflet": "https://www.welcometothejungle.com/en/companies/sifflet/jobs",
    "SnapLogic": "https://www.snaplogic.com/company/careers",
    "Snowflake": "https://careers.snowflake.com/",
    "Solidatus": "https://solidatus.bamboohr.com/",
    "Syniti": "https://careers.syniti.com/",
    "Tencent Cloud": "https://careers.tencent.com/en-us/search.html",
    "Teradata": "https://careers.teradata.com/jobs",
    "Yellowbrick": "https://yellowbrick.com/careers/",
    "Vertica": "https://careers.opentext.com/us/en",
    "Pentaho": "https://www.hitachivantara.com/en-us/company/careers/job-search"
}

COMPANIES = {k: v for k, v in COMPANIES.items() if v}

# --- Cleaning patterns ---
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
BAD_TITLE_PATTERNS = [
    r"learn more", r"apply", r"view all", r"product", r"solution", r"privacy", r"cookie", r"legal",
    r"contact", r"help", r"docs", r"resources", r"pricing", r"features"
]
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English"]
LANG_RE = re.compile(r"\b(" + "|".join(LANG_TAGS) + r")\b", re.I)

def clean_title(raw):
    if not raw:
        return ""
    t = raw.strip()
    for pat in BAD_TITLE_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)
    t = LANG_RE.sub("", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip(" -:,")


def is_likely_job_link(href, text):
    if not href or IMAGE_EXT.search(href):
        return False
    low = (text or href).lower()
    ats_indicators = ["workday", "greenhouse", "lever.co", "bamboohr", "ashby", "myworkdayjobs", "/jobs/", "/apply/"]
    return any(x in low for x in ats_indicators)


def extract_posting_date_from_html(content):
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', content)
    if m:
        return m.group(1).split("T")[0]
    m2 = re.search(r"<time[^>]+datetime=['\"]([^'\"]+)['\"]", content)
    if m2:
        return m2.group(1).split("T")[0]
    return ""


def normalize_link(base, href):
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    if not urlparse(href).netloc:
        return urljoin(base, href)
    return href


def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()

        for company, url in COMPANIES.items():
            print(f"[INFO] Scraping {company} -> {url}")
            if not safe_goto(page, url):
                continue

            anchors = page.query_selector_all("a[href]")
            links = []

            for a in anchors:
                href = a.get_attribute("href")
                text = (a.inner_text() or "").strip()
                full = normalize_link(url, href)
                if not full:
                    continue
                if is_likely_job_link(full, text):
                    links.append((full, text))

            seen = set()
            for link, title_text in links:
                if link in seen:
                    continue
                seen.add(link)

                if not safe_goto(page, link):
                    continue

                html = page.content()
                cleaned_title = clean_title(title_text)
                posted = extract_posting_date_from_html(html)

                loc = ""
                try:
                    el = page.query_selector(".location, .job-location, span.location")
                    if el:
                        loc = el.inner_text().strip()
                except:
                    pass

                rows.append({
                    "Company": company,
                    "Job Title": cleaned_title or title_text,
                    "Job Link": link,
                    "Location": loc,
                    "Posting Date": posted
                })
                time.sleep(0.25)

        browser.close()

    with open("jobs_tighter.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Wrote {len(rows)} rows to jobs_tighter.csv")


if __name__ == "__main__":
    scrape()
