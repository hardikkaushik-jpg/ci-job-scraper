# jobs_smart.py
# Hybrid job scraper using direct ATS feeds + Playwright fallback
# Outputs jobs_clean.csv with proper job postings

import requests, csv, re, json, time
from datetime import datetime
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---- CONFIG ----
COMPANIES = {
    "Alation": "https://alation.wd503.myworkdayjobs.com/ExternalSite",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://www.anomalo.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://jobs.ashbyhq.com/coalesce",
    "Collibra": "https://www.collibra.com/company/careers",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Exasol": "https://careers.exasol.com/",
    "Firebolt": "https://www.comeet.com/jobs/firebolt",
    "MariaDB": "https://boards.eu.greenhouse.io/mariadbplc",
    "Matillion": "https://www.matillion.com/careers",
    "MongoDB": "https://www.mongodb.com/company/careers",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Precisely": "https://www.precisely.com/careers-and-culture/us-jobs",
    "Qlik": "http://careerhub.qlik.com/careers",
    "Sifflet": "https://jobs.lever.co/sifflet",
    "SnapLogic": "https://www.snaplogic.com/company/careers",
    "Solidatus": "https://solidatus.bamboohr.com/jobs",
    "Syniti": "https://careers.syniti.com/content/Careers/",
    "Yellowbrick": "https://yellowbrick.com/careers/"
}

# ---- ATS DETECTION ----
def detect_ats(url):
    u = url.lower()
    if "greenhouse.io" in u or "boards.greenhouse" in u: return "greenhouse"
    if "myworkdayjobs" in u or "wd" in u: return "workday"
    if "lever.co" in u: return "lever"
    if "ashbyhq" in u: return "ashby"
    if "bamboohr" in u: return "bamboohr"
    return None

# ---- FETCHERS ----
def fetch_greenhouse(base):
    api = base.rstrip("/") + "/jobs/feed/json"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for j in data:
        out.append({
            "Company": urlparse(base).netloc.split(".")[0].capitalize(),
            "Job Title": j.get("title",""),
            "Job Link": j.get("absolute_url",""),
            "Location": (j.get("location","") or {}).get("name","") if isinstance(j.get("location"), dict) else j.get("location",""),
            "Posting Date": j.get("updated_at","")[:10]
        })
    return out

def fetch_lever(base):
    api = base.rstrip("/") + "?mode=json"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for j in data:
        out.append({
            "Company": urlparse(base).netloc.split(".")[0].capitalize(),
            "Job Title": j.get("text",""),
            "Job Link": j.get("hostedUrl",""),
            "Location": j.get("categories",{}).get("location",""),
            "Posting Date": j.get("createdAt","")[:10]
        })
    return out

def fetch_ashby(base):
    api = base.rstrip("/") + ".json"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for j in data.get("jobs", []):
        out.append({
            "Company": urlparse(base).netloc.split(".")[0].capitalize(),
            "Job Title": j.get("title",""),
            "Job Link": j.get("url",""),
            "Location": j.get("location",""),
            "Posting Date": j.get("updatedAt","")[:10]
        })
    return out

def fetch_bamboohr(base):
    api = base.rstrip("/") + "/feed/json"
    r = requests.get(api, timeout=20)
    r.raise_for_status()
    data = r.json()
    out = []
    for j in data:
        out.append({
            "Company": urlparse(base).netloc.split(".")[0].capitalize(),
            "Job Title": j.get("jobOpeningName",""),
            "Job Link": j.get("jobOpeningUrl",""),
            "Location": j.get("location",""),
            "Posting Date": j.get("publishedDate","")[:10]
        })
    return out

def fetch_workday(base):
    api = base.rstrip("/") + "/jobs?limit=50"
    r = requests.get(api, timeout=20)
    if r.status_code != 200: return []
    data = r.text
    out = []
    for m in re.finditer(r'"title":"([^"]+)".*?"externalPath":"([^"]+)"', data):
        title, path = m.groups()
        link = base.rstrip("/") + path
        out.append({
            "Company": urlparse(base).netloc.split(".")[0].capitalize(),
            "Job Title": title,
            "Job Link": link,
            "Location": "",
            "Posting Date": ""
        })
    return out

# ---- PLAYWRIGHT FALLBACK ----
def scrape_playwright(url, company):
    rows = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            page = browser.new_page()
            page.goto(url, timeout=50000)
            page.wait_for_load_state("networkidle")
            anchors = page.query_selector_all("a[href]")
            for a in anchors:
                href = a.get_attribute("href") or ""
                text = (a.inner_text() or "").strip()
                if any(x in href.lower() for x in ["/job", "/careers/", "greenhouse", "workday", "lever", "ashby", "bamboohr"]):
                    if len(text.split()) < 15 and re.search(r"\b(engineer|manager|analyst|developer|product|sales|consultant)\b", text, re.I):
                        rows.append({
                            "Company": company,
                            "Job Title": text,
                            "Job Link": href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/"),
                            "Location": "",
                            "Posting Date": ""
                        })
            browser.close()
    except Exception as e:
        print(f"[WARN] Playwright failed for {url}: {e}")
    return rows

# ---- MAIN ----
def main():
    all_jobs = []
    for company, url in COMPANIES.items():
        print(f"[INFO] {company} -> {url}")
        ats = detect_ats(url)
        try:
            if ats == "greenhouse": jobs = fetch_greenhouse(url)
            elif ats == "lever": jobs = fetch_lever(url)
            elif ats == "ashby": jobs = fetch_ashby(url)
            elif ats == "bamboohr": jobs = fetch_bamboohr(url)
            elif ats == "workday": jobs = fetch_workday(url)
            else: jobs = scrape_playwright(url, company)
        except Exception as e:
            print(f"[WARN] failed {company}: {e}")
            jobs = scrape_playwright(url, company)
        all_jobs.extend(jobs)
        time.sleep(0.5)

    # write CSV
    with open("jobs_clean.csv","w",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company","Job Title","Job Link","Location","Posting Date"])
        writer.writeheader()
        writer.writerows(all_jobs)
    print(f"[OK] wrote {len(all_jobs)} real jobs to jobs_clean.csv")

if __name__ == "__main__":
    main()
