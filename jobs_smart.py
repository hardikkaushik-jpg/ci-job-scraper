# jobs_smart.py
# Final Smart ATS Scraper with Date & Location Intelligence
# Outputs jobs_clean.csv including "Days Since Posted"

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import re, csv, time
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

# --- CONFIG: ALL COMPETITORS ---
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
    "CastorDoc (Coalesce)": "https://jobs.ashbyhq.com/coalesce",
    "Cloudera": "https://cloudera.wd5.myworkdayjobs.com/External_Career",
    "Collibra": "https://www.collibra.com/company/careers/jobs",
    "Couchbase": "https://www.couchbase.com/careers/open-positions",
    "Data.World (ServiceNow)": "https://data.world/company/careers",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://careers.datadoghq.com/",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube": "https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol": "https://careers.exasol.com/en/jobs",
    "Firebolt": "https://www.comeet.com/jobs/firebolt",
    "Fivetran": "https://fivetran.com/careers/open-positions",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs",
    "MariaDB": "https://job-boards.eu.greenhouse.io/mariadbplc",
    "Matillion": "https://jobs.lever.co/matillion",
    "MongoDB": "https://www.mongodb.com/company/careers/jobs",
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
    "Solidatus": "https://solidatus.bamboohr.com/jobs/",
    "SQLite": "https://www.sqlite.org/careers.html",
    "Syniti": "https://careers.syniti.com/jobs",
    "Tencent Cloud": "https://careers.tencent.com/en-us/search.html",
    "Teradata": "https://careers.teradata.com/jobs",
    "Yellowbrick": "https://yellowbrick.com/careers/",
    "Vertica": "https://careers.opentext.com/us/en",
    "Pentaho (Hitachi)": "https://www.hitachivantara.com/en-us/company/careers/job-search"
}

# --- HELPERS ---
def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def is_likely_job_link(href, text):
    if not href:
        return False
    low = (text or href).lower()
    positives = [
        '/jobs', '/job', '/careers', '/openings', '/positions',
        'lever.co', 'greenhouse', 'bamboohr', 'myworkdayjobs',
        'gr8people', 'ashby', 'comeet', 'workable', 'jobvite'
    ]
    return any(p in low for p in positives)

def clean_title(title):
    if not title:
        return ""
    t = re.sub(r'\s+', ' ', title).strip()
    # remove marketing/junk phrases
    t = re.sub(r'(?i)\b(Learn More|Apply|Apply Now|Read More|View Role|View Job|Learn More & Apply)\b', '', t)
    t = re.sub(r'Learn More\s*&?\s*Apply', '', t, flags=re.I)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -•–")
    return t

def extract_posting_date_from_html(html):
    """Extract datePosted, <time>, or 'Posted X days ago'."""
    # 1. JSON-LD
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1).split('T')[0]
    # 2. <time datetime="">
    m2 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html)
    if m2:
        return m2.group(1).split('T')[0]
    # 3. Workday “Posted on Month Day, Year”
    m3 = re.search(r'Posted\s+on\s+([A-Za-z]+\s+\d{1,2},\s*\d{4})', html)
    if m3:
        try:
            return datetime.strptime(m3.group(1), "%B %d, %Y").date().isoformat()
        except Exception:
            pass
    # 4. “Posted X days ago”
    m4 = re.search(r'Posted\s*(\d+)\s*day', html)
    if m4:
        days = int(m4.group(1))
        date = datetime.utcnow() - timedelta(days=days)
        return date.date().isoformat()
    return ""

def extract_location_from_title(title):
    if not title:
        return ""
    loc_pattern = re.compile(
        r'\b(Remote|Hybrid|Onsite|United States|US|UK|United Kingdom|India|Germany|France|Canada|Singapore|Australia|Netherlands|Spain|Italy|Poland|Belgium|Mexico|Brazil|Japan|Ireland|Sweden|Finland|Norway|Switzerland|Austria)\b',
        re.I
    )
    matches = loc_pattern.findall(title)
    if matches:
        return ", ".join(sorted(set([m.strip() for m in matches])))
    return ""

def dedupe_keep_latest(rows):
    seen = set()
    result = []
    for r in rows:
        link = r["Job Link"]
        if link not in seen:
            result.append(r)
            seen.add(link)
    return result

# --- MAIN SCRAPER ---
def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        for company, url in COMPANIES.items():
            print(f"[INFO] Scraping {company} → {url}")
            try:
                page.goto(url, timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
            except Exception as e:
                print(f"[WARN] Timeout or error loading {url}: {e}")
                continue

            anchors = page.query_selector_all("a[href]")
            for a in anchors:
                try:
                    href = a.get_attribute("href") or ""
                    text = (a.inner_text() or "").strip()
                except Exception:
                    continue

                full = normalize_link(url, href)
                if not is_likely_job_link(full, text):
                    continue

                cleaned_title = clean_title(text)
                location = extract_location_from_title(cleaned_title)
                posting_date = ""

                try:
                    page.goto(full, timeout=40000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    html = page.content()
                    posting_date = extract_posting_date_from_html(html)
                except PWTimeout:
                    pass
                except Exception:
                    pass

                # compute days since posted
                days_since = ""
                if posting_date:
                    try:
                        post_dt = datetime.fromisoformat(posting_date)
                        days_since = (datetime.utcnow().date() - post_dt.date()).days
                    except Exception:
                        days_since = ""

                rows.append({
                    "Company": company,
                    "Job Title": cleaned_title,
                    "Job Link": full,
                    "Location": location,
                    "Posting Date": posting_date,
                    "Days Since Posted": days_since
                })

                time.sleep(0.2)

        browser.close()

    rows = dedupe_keep_latest(rows)
    out = "jobs_clean.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date", "Days Since Posted"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Wrote {len(rows)} jobs → {out}")

if __name__ == "__main__":
    scrape()
