from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
import time, csv, re
from datetime import datetime
from urllib.parse import urljoin

# -----------------------------
# CLEAN COMPANY URL MAP (no indent errors)
# -----------------------------
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
    "Snowflake": ["https://careers.snowflake.com/us/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"]
}

# Cleaner for job titles
def clean_title(t):
    if not t:
        return ""
    t = t.replace("\n", " ").strip()
    t = re.sub(r"Learn More.*$", "", t, flags=re.I)
    t = re.sub(r"Apply.*$", "", t, flags=re.I)
    t = re.sub(r"Location.*$", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip(" -·,:")

def extract_date(text):
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    return m.group(1) if m else ""

def extract_location(txt):
    m = re.search(r"(Remote|Hybrid|Onsite|United States|US|UK|India|Singapore|Canada|Germany|France|Bulgaria|Serbia|Croatia|Romania|Estonia|Spain|Japan|Australia)", txt, re.I)
    return m.group(1) if m else ""

# Main scraper
def scrape():
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for company, links in COMPANIES.items():
            for url in links:
                print(f"[SCRAPING] {company} → {url}")

                try:
                    page.goto(url, timeout=40000)
                    page.wait_for_load_state("networkidle")
                except Exception:
                    print(f"[WARN] Page failed: {url}")
                    continue

                html = page.content()
                soup = BeautifulSoup(html, "lxml")

                # Pick ALL clickable job-like links
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    text = clean_title(a.get_text(" ", strip=True))

                    if not href or len(text) < 3:
                        continue

                    # Skip garbage
                    if any(x in text.lower() for x in ["policy", "privacy", "legal", "contact", "dashboard", "profile"]):
                        continue

                    # Keep only job links
                    if re.search(r"(job|jobs|career|apply|opening|position)", href, re.I):
                        link = href if href.startswith("http") else urljoin(url, href)

                        loc = extract_location(text)
                        date = extract_date(text)

                        rows.append({
                            "Company": company,
                            "Job Title": text,
                            "Job Link": link,
                            "Location": loc,
                            "Posting Date": date
                        })

                time.sleep(0.2)

        browser.close()

    # Save final CSV
    out = "jobs_final_hard.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date"])
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print(f"[DONE] {len(rows)} jobs → {out}")


if __name__ == "__main__":
    scrape()
