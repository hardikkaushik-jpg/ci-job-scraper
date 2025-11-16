# jobs_smart.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages
# Output -> jobs_final_hard.csv
# Usage:
#   pip install -r requirements.txt
#   python jobs_smart.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys
from datetime import datetime, date, timedelta

# ------------------ CONFIG: canonical career URLs (ONLY these are used) ------------------
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
# ---------------------------------------------------------------------------------

# Timeouts / throttles (tune if needed)
PAGE_NAV_TIMEOUT = 35000      # main listing page navigation timeout (ms)
PAGE_IDLE_TIMEOUT = 12000     # wait for network idle for listing (ms)
DETAIL_NAV_TIMEOUT = 20000    # detail page nav timeout (ms)
DETAIL_IDLE_TIMEOUT = 8000    # wait for network idle on detail (ms)
SLEEP_BETWEEN_REQUESTS = 0.25

# heuristics / regex
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply now', r'view all', r'view openings', r'location', r'locations',
    r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs',
    r'features', r'pricing', r'resources', r'news', r'events'
]
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia)\b', re.I)

def clean_title(raw: str) -> str:
    if not raw:
        return ""
    t = re.sub(r'[\r\n]+', ' ', raw)
    t = re.sub(r'\s+', ' ', t).strip()
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply\s*now.*', '', t, flags=re.I)
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def normalize_link(base: str, href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def is_likely_job_anchor(href: str, text: str) -> bool:
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False
    low = (text or href or "").lower()
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'apply', 'greenhouse', 'lever.co', 'boards.greenhouse', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'job-boards', 'job-boards.eu', 'workday']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    if text and 2 <= len(text.split()) <= 9 and re.search(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa)\b', text, re.I):
        return True
    return False

def extract_date_from_html(html: str) -> str:
    if not html:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1).split("T")[0]
    m2 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m2:
        try:
            return datetime.fromisoformat(m2.group(1)).date().isoformat()
        except:
            return m2.group(1)
    m3 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m3:
        return m3.group(1).split("T")[0]
    m4 = re.search(r'posted[:\s-]*([A-Za-z0-9 ,\-]+)', html, re.I)
    if m4:
        iso = re.search(r'(\d{4}-\d{2}-\d{2})', m4.group(1))
        if iso:
            return iso.group(1)
        # "Posted X days ago" -> convert
        mdays = re.search(r'(\d+)\s+days?\s+ago', m4.group(1), re.I)
        if mdays:
            d = date.today() - timedelta(days=int(mdays.group(1)))
            return d.isoformat()
    return ""

def days_since(iso: str) -> str:
    if not iso:
        return ""
    try:
        d = datetime.fromisoformat(iso).date()
    except:
        try:
            d = datetime.strptime(iso, "%Y-%m-%d").date()
        except:
            return ""
    return str((date.today() - d).days)

def parse_listing_for_links(base_url: str, html: str):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = (a.get_text(" ", strip=True) or "")
        href_abs = normalize_link(base_url, href)
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text))
    # also try common job card selectors
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .gh-job"):
        a = el.find("a", href=True)
        if a:
            href = normalize_link(base_url, a.get("href"))
            text = (a.get_text(" ", strip=True) or el.get_text(" ", strip=True))
            if is_likely_job_anchor(href, text):
                candidates.append((href, text))
    # dedupe while preserving order
    seen = set(); out = []
    for href, text in candidates:
        if not href:
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text))
    return out

def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, idle_timeout=PAGE_IDLE_TIMEOUT):
    """Load page and return page.content() or '' on failure."""
    try:
        # set a short timeout and listen for downloads
        download_triggered = {"flag": False}
        def on_download(d):
            download_triggered["flag"] = True
            try:
                print(f"[WARN] download triggered for {url} -> skipping detail.")
            except:
                pass
        page.on("download", on_download)
        page.goto(url, timeout=nav_timeout, wait_until="networkidle")
        # small wait to let js populate
        page.wait_for_timeout(300)
        html = page.content()
        if download_triggered["flag"]:
            return ""   # skip pages that start downloads
        return html
    except PWTimeout:
        print(f"[WARN] timeout loading {url}")
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            html = page.content()
            return html
        except Exception as e:
            print(f"[WARN] fallback load failed for {url} -> {e}")
            return ""
    except Exception as e:
        print(f"[WARN] failed to fetch {url} -> {e}")
        return ""

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        # Set global timeouts a bit lower
        page.set_default_navigation_timeout(PAGE_NAV_TIMEOUT)
        for company, url_list in COMPANIES.items():
            # url_list can be list of urls per company
            for url in url_list:
                print(f"[SCRAPING] {company} -> {url}")
                html = fetch_page_content(page, url)
                if not html:
                    print(f"[WARN] no html for {company} ({url}) -- skipping this url")
                    continue

                # parse candidate job links from HTML
                candidates = parse_listing_for_links(url, html)

                # if page loads an iframe ATS or embedded widget, try to load iframe srcs
                if not candidates:
                    soup = BeautifulSoup(html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards", "jobs.lever.co")):
                            src_full = normalize_link(url, src)
                            print(f"[INFO] found iframe src -> {src_full}")
                            html2 = fetch_page_content(page, src_full)
                            if html2:
                                candidates += parse_listing_for_links(src_full, html2)

                # final fallback: scan anchors one more time but aggressively
                if not candidates:
                    soup = BeautifulSoup(html, "lxml")
                    for a in soup.find_all("a", href=True):
                        href = normalize_link(url, a.get("href"))
                        text = (a.get_text(" ", strip=True) or "")
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text))

                # filter candidates (avoid the listing homepage itself, product pages etc)
                filtered = []
                for href, text in candidates:
                    if not href:
                        continue
                    if href.rstrip("/") == url.rstrip("/"):
                        continue
                    if IMAGE_EXT.search(href):
                        continue
                    # avoid obvious marketing endpoints
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                        # allow only if anchor text looks like a job role
                        if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
                            continue
                    filtered.append((href, text))

                # visit each candidate detail page to get cleaned title + location + date
                for link, anchor_text in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    job_title = clean_title(anchor_text)
                    job_location = ""
                    posting_date = ""

                    # sometimes anchor contains role + location on separate lines
                    if '\n' in anchor_text and not job_location:
                        pieces = [p.strip() for p in anchor_text.splitlines() if p.strip()]
                        if len(pieces) >= 2 and re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta)\b', pieces[-1], re.I):
                            job_location = pieces[-1]
                            if not job_title:
                                job_title = clean_title(pieces[0])

                    detail_html = fetch_page_content(page, link, nav_timeout=DETAIL_NAV_TIMEOUT, idle_timeout=DETAIL_IDLE_TIMEOUT)
                    if detail_html:
                        soup = BeautifulSoup(detail_html, "lxml")
                        # prefer H1 if anchor title noisy
                        if (not job_title or len(job_title) < 3) and soup.find("h1"):
                            job_title = clean_title(soup.find("h1").get_text(" ", strip=True))
                        # try common location selectors
                        for sel in ["span.location", ".job-location", ".location", "[data-job-location]", ".posting-location", ".job_meta_location", ".job-location__text"]:
                            el = soup.select_one(sel)
                            if el and el.get_text(strip=True):
                                job_location = el.get_text(" ", strip=True).strip()
                                break
                        # extract posting date
                        posted = extract_date_from_html(detail_html)
                        if posted:
                            posting_date = posted
                        else:
                            txt = soup.get_text(" ", strip=True)
                            posted_local = extract_date_from_html(txt)
                            if posted_local:
                                posting_date = posted_local

                    job_title = clean_title(job_title)
                    job_title = re.sub(r'learn more.*', '', job_title, flags=re.I).strip()
                    job_title = re.sub(r'apply.*', '', job_title, flags=re.I).strip()
                    job_title = LANG_RE.sub('', job_title).strip()

                    rows.append({
                        "Company": company,
                        "Job Title": job_title,
                        "Job Link": link,
                        "Location": job_location,
                        "Posting Date": posting_date,
                        "Days Since Posted": days_since(posting_date) if posting_date else ""
                    })

        browser.close()

    # dedupe by Job Link
    seen = set(); out = []
    for r in rows:
        lk = (r.get("Job Link") or "").strip()
        if not lk:
            continue
        if lk in seen:
            continue
        seen.add(lk)
        out.append(r)

    out_file = "jobs_final_hard.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"])
        writer.writeheader()
        for r in out:
            writer.writerow(r)
    print(f"[OK] wrote {len(out)} rows -> {out_file}")

if __name__ == "__main__":
    try:
        scrape()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
