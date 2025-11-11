# jobs_scraper.py (improved)
import requests, json, re
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
from datetime import datetime

COMPANIES = {
   "Airtable": "https://airtable.com/careers",
    "Alation": "https://www.alation.com/careers/all-careers/",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
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
    "IBM": "https://www.ibm.com/careers/us-en/search/",
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs?utm_medium=Direct",
    "MariaDB": "https://mariadb.com/about/careers/",
    "Matillion": "https://www.matillion.com/careers",
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

HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; job-scraper/1.0; +mailto:you@example.com)"}

def fetch_html(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[WARN] fetch failed for {url}: {e}")
        return ""

def parse_json_ld(html, base_url):
    out = []
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string)
        except:
            continue
        # jobPosting can be a list or dict
        if isinstance(data, dict) and data.get("@type","").lower() == "jobposting":
            title = data.get("title","")
            link = data.get("url") or base_url
            out.append({"title": title, "link": link})
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type","").lower()=="jobposting":
                    title = item.get("title","")
                    link = item.get("url") or base_url
                    out.append({"title": title, "link": link})
    return out

# site specific extractors
def extract_jobs_lever(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows=[]
    for a in soup.select("a[href]"):
        href = a["href"]
        if "/jobs/" in href or href.startswith("https://jobs.lever.co"):
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            title = a.get_text(" ", strip=True)
            rows.append({"title": title, "link": full})
    return rows

def extract_jobs_greenhouse(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    rows=[]
    for a in soup.select("a[href*='/jobs/']"):
        full = a["href"] if urlparse(a["href"]).netloc else urljoin(base_url, a["href"])
        title = a.get_text(" ", strip=True)
        rows.append({"title": title, "link": full})
    return rows

def extract_jobs_workday(base_url, html):
    # workday often contains links to ExternalSite/job/... or structured anchors
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if "/job/" in href or "Workday" in href or "wd3.myworkdayjobs" in href or "/external" in href.lower():
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            title = a.get_text(" ", strip=True)
            rows.append({"title": title, "link": full})
    return rows

def extract_jobs_ashby(base_url, html):
    # ashby job pages have articles or role anchors
    soup = BeautifulSoup(html, "lxml")
    rows=[]
    for a in soup.select("a[href]"):
        href=a["href"]
        if "/jobs/" in href or "ashby" in href:
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            title = a.get_text(" ", strip=True)
            rows.append({"title": title, "link": full})
    return rows

def extract_jobs_generic(base_url, html):
    # priority: JSON-LD
    out = parse_json_ld(html, base_url)
    if out:
        return out
    # site patterns
    if "lever.co" in base_url or "jobs.lever" in base_url:
        return extract_jobs_lever(base_url, html)
    if "greenhouse" in base_url or "boards.greenhouse" in base_url:
        return extract_jobs_greenhouse(base_url, html)
    if "workday" in base_url or "myworkdayjobs" in base_url:
        return extract_jobs_workday(base_url, html)
    if "ashby" in base_url:
        return extract_jobs_ashby(base_url, html)
    # fallback: anchors with clear job patterns
    soup = BeautifulSoup(html, "lxml")
    rows=[]
    for a in soup.find_all("a", href=True):
        href=a["href"].strip()
        text=a.get_text(" ", strip=True)
        if href.startswith("mailto:") or href.startswith("tel:"):
            continue
        if re.search(r"/jobs|/careers|/job/|open-role|open-roles|/positions|vacancies", href+text, re.I):
            full = href if urlparse(href).netloc else urljoin(base_url, href)
            rows.append({"title": text[:250], "link": full})
    return rows

def guess_date_from_detail(link):
    html = fetch_html(link)
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    # look for date patterns
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m: return m.group(1)
    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y").date().isoformat()
        except:
            try:
                return datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
            except:
                return m.group(1)
    return ""

def is_valid_row(title, link):
    if not link or link.strip()=="":
        return False
    l = link.lower()
    if l.startswith("mailto:") or l.startswith("tel:") or "linkedin.com/share" in l:
        return False
    if re.search(r"contact|about|privacy|terms|help|support|blog|press", title+" "+l, re.I):
        return False
    if re.search(r"/jobs|/careers|/job/|open-role|open-roles|/positions|vacanc", l, re.I):
        return True
    if re.search(r"\b(engineer|developer|manager|analyst|product|data|scientist|director|lead|principal|intern|consultant)\b", title, re.I):
        return True
    return False

def scrape_one_company(name, url):
    html = fetch_html(url)
    if not html:
        return []
    candidates = extract_jobs_generic(url, html)
    results=[]
    for c in candidates:
        title = (c.get("title") or "").strip()
        link = c.get("link") or url
        if not link:
            continue
        # normalize
        link = link.split("?")[0]
        if not is_valid_row(title, link):
            continue
        posted = guess_date_from_detail(link)
        results.append({"Company": name, "Job Title": title or "(no title)", "Job Link": link, "Location": "", "Posting Date": posted})
    return results

def main():
    all_rows=[]
    for name,url in COMPANIES.items():
        print("Scraping", name, url)
        try:
            rows = scrape_one_company(name, url)
            print(" -> found", len(rows))
            all_rows += rows
        except Exception as e:
            print("ERROR:", e)
    if not all_rows:
        print("No rows, exiting.")
        return
    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["Company","Job Link"], inplace=True)
    df.to_csv("2jobs.csv", index=False)
    print("Wrote 2jobs.csv:", len(df))

if __name__=="__main__":
    main()
