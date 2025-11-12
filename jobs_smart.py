# jobs_smart.py
# Hybrid Smart Job Scraper (Static + Dynamic ATS)
# Combines requests/BeautifulSoup for static pages and Playwright for dynamic ATS
# Output: jobs_clean.csv with Location, Posting Date, Days Since Posted, and Last Checked

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import requests, re, csv, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

# --- CONFIG ---
STATIC_COMPANIES = {
    "Boomi": "https://boomi.com/company/careers/",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "Yellowbrick": "https://yellowbrick.com/careers/",
    "Vertica": "https://careers.opentext.com/us/en/home",
    "SnapLogic": "https://www.snaplogic.com/company/careers/job-listings",
}

DYNAMIC_COMPANIES = {
    "Alation": "https://alation.wd503.myworkdayjobs.com/ExternalSite",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://boards.greenhouse.io/anomalojobs",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://careers.datadoghq.com/",
    "Exasol": "https://careers.exasol.com/en/jobs",
    "MariaDB": "https://job-boards.eu.greenhouse.io/mariadbplc",
    "Matillion": "https://jobs.lever.co/matillion",
    "MongoDB": "https://www.mongodb.com/company/careers/jobs",
    "Monte Carlo": "https://jobs.ashbyhq.com/montecarlodata",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Syniti": "https://careers.syniti.com/jobs",
    "Qlik": "http://careerhub.qlik.com/careers",
    "SAP": "https://jobs.sap.com/",
}

# --- HELPERS ---
def clean_title(text):
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'(?i)\b(Learn More|Apply|Apply Now|Read More|View Role|View Job|Learn More & Apply)\b', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip(" -•–")

def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def extract_posting_date_from_html(html):
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1).split('T')[0]
    m2 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html)
    if m2:
        return m2.group(1).split('T')[0]
    m3 = re.search(r'Posted\s*(\d+)\s*day', html)
    if m3:
        days = int(m3.group(1))
        date = datetime.utcnow() - timedelta(days=days)
        return date.date().isoformat()
    return ""

def extract_location(title):
    if not title:
        return ""
    matches = re.findall(r'\b(Remote|Hybrid|Onsite|United States|US|UK|United Kingdom|India|Germany|France|Canada|Singapore|Australia|Netherlands|Spain|Italy|Poland|Belgium|Brazil|Japan)\b', title, re.I)
    return ", ".join(sorted(set(matches)))

def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        if r["Job Link"] not in seen:
            out.append(r)
            seen.add(r["Job Link"])
    return out

# --- STATIC SCRAPER ---
def scrape_static(name, url):
    print(f"[STATIC] {name} → {url}")
    jobs = []
    try:
        html = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        anchors = soup.find_all("a", href=True)
        for a in anchors:
            text = clean_title(a.get_text(strip=True))
            href = normalize_link(url, a["href"])
            if not text or not href:
                continue
            if any(k in href.lower() for k in ["job", "career", "opening", "position", "apply"]):
                jobs.append({
                    "Company": name,
                    "Job Title": text,
                    "Job Link": href,
                    "Location": extract_location(text),
                    "Posting Date": "",
                    "Days Since Posted": "",
                    "Last Checked": datetime.utcnow().isoformat()
                })
    except Exception as e:
        print(f"[WARN] Static failed for {name}: {e}")
    return jobs

# --- DYNAMIC SCRAPER ---
def scrape_dynamic(name, url):
    print(f"[DYNAMIC] {name} → {url}")
    jobs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception as e:
            print(f"[WARN] load fail {url}: {e}")
            browser.close()
            return jobs

        anchors = page.query_selector_all("a[href]")
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                text = clean_title(a.inner_text() or "")
            except:
                continue
            full = normalize_link(url, href)
            if not text or not full:
                continue
            if any(k in full.lower() for k in ["job", "opening", "lever.co", "greenhouse", "workday", "bamboohr", "ashby", "apply"]):
                loc = extract_location(text)
                posting_date = ""
                try:
                    page.goto(full, timeout=30000)
                    page.wait_for_load_state("domcontentloaded", timeout=10000)
                    html = page.content()
                    posting_date = extract_posting_date_from_html(html)
                except:
                    pass
                days_since = ""
                if posting_date:
                    try:
                        post_dt = datetime.fromisoformat(posting_date)
                        days_since = (datetime.utcnow().date() - post_dt.date()).days
                    except:
                        days_since = ""
                jobs.append({
                    "Company": name,
                    "Job Title": text,
                    "Job Link": full,
                    "Location": loc,
                    "Posting Date": posting_date,
                    "Days Since Posted": days_since,
                    "Last Checked": datetime.utcnow().isoformat()
                })
        browser.close()
    return jobs

# --- MAIN ---
def main():
    all_jobs = []
    for name, url in STATIC_COMPANIES.items():
        all_jobs.extend(scrape_static(name, url))
    for name, url in DYNAMIC_COMPANIES.items():
        all_jobs.extend(scrape_dynamic(name, url))

    all_jobs = dedupe(all_jobs)
    with open("jobs_clean.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date", "Days Since Posted", "Last Checked"])
        writer.writeheader()
        writer.writerows(all_jobs)
    print(f"[OK] Saved {len(all_jobs)} rows → jobs_clean.csv")

if __name__ == "__main__":
    main()
