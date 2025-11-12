# jobs_smart.py — full 56-company version (Smart ATS Scraper)
# Uses Playwright for dynamic ATS pages and fallback logic for static sites.
# Output: jobs_clean.csv

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import re, csv, time
from urllib.parse import urljoin, urlparse
from datetime import datetime

# -------------------------------
# ✅ Full 56 Competitor Career Pages
# -------------------------------
COMPANIES = {
    "Airtable": "https://airtable.com/careers",
    "Alation": "https://alation.wd503.myworkdayjobs.com/ExternalSite",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Amazon (AWS)": "https://www.amazon.jobs/en/teams/aws",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://boards.greenhouse.io/anomalojobs",
    "BigEye": "https://www.bigeye.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://coalesce.io/careers/",
    "Cloudera": "https://www.cloudera.com/careers",
    "Collibra": "https://www.collibra.com/company/careers",
    "Couchbase": "https://www.couchbase.com/careers/",
    "Data.World (ServiceNow)": "https://data.world/company/careers",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://careers.datadoghq.com/",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube": "https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol": "https://careers.exasol.com/en/jobs",
    "Firebolt": "https://www.firebolt.io/careers",
    "Fivetran": "https://fivetran.com/careers",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs",
    "MariaDB": "https://job-boards.eu.greenhouse.io/mariadbplc",
    "Matillion": "https://jobs.lever.co/matillion",
    "MongoDB (Engineering)": "https://www.mongodb.com/company/careers/teams/engineering",
    "MongoDB (Marketing)": "https://www.mongodb.com/company/careers/teams/marketing",
    "MongoDB (Sales)": "https://www.mongodb.com/company/careers/teams/sales",
    "MongoDB (Product)": "https://www.mongodb.com/company/careers/teams/product-management-and-design",
    "Monte Carlo": "https://jobs.ashbyhq.com/montecarlodata",
    "Mulesoft": "https://www.mulesoft.com/careers",
    "Nutanix": "https://careers.nutanix.com/en/jobs/",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Oracle": "https://careers.oracle.com/en/sites/jobsearch/jobs",
    "Panoply": "https://sqream.com/careers/",
    "PostgreSQL": "https://www.postgresql.org/about/careers/",
    "Precisely (US)": "https://www.precisely.com/careers-and-culture/us-jobs",
    "Precisely (Int)": "https://www.precisely.com/careers-and-culture/international-jobs",
    "Qlik": "http://careerhub.qlik.com/careers",
    "SAP": "https://jobs.sap.com/",
    "Sifflet": "https://www.welcometothejungle.com/en/companies/sifflet/jobs",
    "SnapLogic": "https://www.snaplogic.com/company/careers/job-listings",
    "Snowflake": "https://careers.snowflake.com/",
    "Solidatus": "https://solidatus.bamboohr.com/jobs",
    "SQLite": "https://www.sqlite.org/careers.html",
    "Syniti": "https://careers.syniti.com/jobs",
    "Tencent Cloud": "https://careers.tencent.com/en-us/search.html",
    "Teradata": "https://careers.teradata.com/jobs",
    "Yellowbrick": "https://yellowbrick.com/careers/",
    "Vertica": "https://careers.opentext.com/us/en/home",
    "Pentaho": "https://www.hitachivantara.com/en-us/company/careers/job-search"
}

# -------------------------------
# 🧠 Helpers
# -------------------------------
BAD_WORDS = [
    "learn more", "apply", "view all", "product", "solution", "connector",
    "pricing", "privacy", "contact", "help", "legal", "resources", "press"
]
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)

def clean_title(t):
    if not t:
        return ""
    t = re.sub(r'\s+', ' ', t).strip()
    for w in BAD_WORDS:
        t = re.sub(w, '', t, flags=re.I)
    t = LANG_RE.sub('', t)
    return re.sub(r'\s{2,}', ' ', t).strip(" -:,\n")

def normalize(base, href):
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def is_job_link(href, text):
    if not href or IMAGE_EXT.search(href):
        return False
    key = href.lower() + (text or "").lower()
    patterns = ["job", "jobs", "careers", "apply", "workday", "greenhouse", "lever", "bamboohr", "ashby", "openings"]
    return any(p in key for p in patterns)

def extract_post_date(html):
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m: return m.group(1).split("T")[0]
    m2 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html)
    if m2: return m2.group(1).split("T")[0]
    return ""

def dedupe(rows):
    seen, out = set(), []
    for r in rows:
        if r["Job Link"] not in seen:
            seen.add(r["Job Link"])
            out.append(r)
    return out

# -------------------------------
# 🚀 Main Scraper
# -------------------------------
def scrape():
    all_rows = []
    failed = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        for name, url in COMPANIES.items():
            print(f"\n[INFO] {name} → {url}")
            try:
                for attempt in range(2):  # retry once
                    try:
                        page.goto(url, timeout=70000)
                        page.wait_for_load_state("networkidle", timeout=60000)
                        break
                    except PWTimeout:
                        print(f"[WARN] Timeout attempt {attempt+1}/2 for {name}")
                else:
                    raise Exception("Timed out twice")

                anchors = page.query_selector_all("a[href]")
                jobs = []
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    text = (a.inner_text() or "").strip()
                    if is_job_link(href, text):
                        full = normalize(url, href)
                        title = clean_title(text)
                        if len(title) > 3 and len(title.split()) <= 15:
                            jobs.append((title, full))

                if not jobs:
                    print(f"[WARN] No jobs found for {name}")
                    failed.append(name)
                    continue

                for title, link in jobs:
                    all_rows.append({
                        "Company": name,
                        "Job Title": title,
                        "Job Link": link,
                        "Location": "",
                        "Posting Date": ""
                    })
                print(f"[OK] {name} → {len(jobs)} jobs")

            except Exception as e:
                print(f"[ERROR] {name}: {e}")
                failed.append(name)
            time.sleep(0.5)

        browser.close()

    # Write output
    all_rows = dedupe(all_rows)
    out = "jobs_clean.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date"])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n✅ DONE — {len(all_rows)} jobs scraped → {out}")
    if failed:
        print("\n⚠️ Failed or empty companies:")
        for f in failed:
            print(" -", f)

if __name__ == "__main__":
    scrape()
