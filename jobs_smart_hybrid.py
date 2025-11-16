# jobs_smart_hybrid.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages
# Balanced hybrid mode: open up to N detail pages per company (10)
# Only opens detail page when location or posting date missing on listing
# Output -> jobs_final_hard.csv
#
# Usage:
#   pip install -r requirements.txt
#   python jobs_smart_hybrid.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, date, timedelta
import re, csv, time, sys

# ------------------ CONFIG: canonical career URLs you gave ------------------
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
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"]
}

# ------------------ PARAMETERS (Hybrid) ------------------
PAGE_NAV_TIMEOUT = 45000     # ms for listing pages
PAGE_IDLE_TIMEOUT = 25000
DETAIL_NAV_TIMEOUT = 20000   # max 20s for detail page
DETAIL_IDLE_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.2
MAX_DETAIL_PER_COMPANY = 10  # your "balanced" limit
MAX_DETAIL_ATTEMPTS_IF_MISSING = 2  # attempts to open detail page when info missing

# ------------------ helpers & patterns ------------------
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply', r'view all', r'view openings', r'location', r'locations',
    r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs',
    r'features', r'pricing', r'resources', r'news', r'events'
]
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia)\b', re.I)

def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'[\r\n]+', ' ', raw)
    t = re.sub(r'\s+', ' ', t).strip()
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply.*', '', t, flags=re.I)
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = re.sub(r'[\u00A0\u200B]', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def extract_date_from_html(html):
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
    # "Posted 21 days ago"
    m4 = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m4:
        days = int(m4.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    return ""

def days_since(iso_date_str):
    if not iso_date_str:
        return ""
    try:
        d = datetime.fromisoformat(iso_date_str).date()
    except:
        try:
            d = datetime.strptime(iso_date_str, "%Y-%m-%d").date()
        except:
            return ""
    return str((date.today() - d).days)

def is_likely_job_anchor(href, text):
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False
    low = (text or href).lower()
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'greenhouse', 'lever.co', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'job-boards']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    if text and 2 <= len(text.split()) <= 10 and re.search(r'\b(engineer|developer|analyst|manager|director|product|scientist|architect|consultant|sales|designer|sre|qa)\b', text, re.I):
        return True
    return False

def parse_listing_for_links(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True) or ""
        href = normalize_link(base_url, a.get("href"))
        if is_likely_job_anchor(href, text):
            candidates.append((href, text))
    # search for job-card blocks
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting"):
        a = el.find("a", href=True)
        if a:
            text = a.get_text(" ", strip=True) or el.get_text(" ", strip=True)
            href = normalize_link(base_url, a.get("href"))
            if is_likely_job_anchor(href, text):
                candidates.append((href, text))
    # dedupe preserving order
    seen = set()
    out = []
    for href, txt in candidates:
        if href in seen:
            continue
        seen.add(href)
        out.append((href, txt))
    return out

def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, idle_timeout=PAGE_IDLE_TIMEOUT, wait_until="networkidle"):
    try:
        page.goto(url, timeout=nav_timeout, wait_until=wait_until)
        page.wait_for_load_state("networkidle", timeout=idle_timeout)
        return page.content()
    except PWTimeout:
        # try domcontentloaded fallback
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] timeout/fallback failed for {url} -> {e}")
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

        for company, urls in COMPANIES.items():
            print(f"[SCRAPING] {company}")
            candidates = []
            for url in urls:
                html = fetch_page_content(page, url)
                if not html:
                    print(f"[WARN] no html for {company} at {url}")
                    continue
                found = parse_listing_for_links(url, html)
                if found:
                    candidates.extend(found)
                else:
                    # check for iframe ATS embeds
                    soup = BeautifulSoup(html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = normalize_link(url, iframe.get("src"))
                        if any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards")):
                            subhtml = fetch_page_content(page, src)
                            if subhtml:
                                candidates.extend(parse_listing_for_links(src, subhtml))

            # dedupe preserving order
            seen = set()
            filtered = []
            for href, text in candidates:
                if href in seen:
                    continue
                seen.add(href)
                # ignore same-as-listing landing
                if any(href.rstrip('/') == u.rstrip('/') for u in urls):
                    continue
                # ignore obvious marketing/product pages
                if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                    if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
                        continue
                filtered.append((href, text))

            detail_opens = 0
            attempts_when_missing = 0

            # For each candidate: try to extract title/location/date from anchor_text; open detail only if necessary
            for link, anchor_text in filtered:
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                # basic extraction from anchor_text (common pattern: "Job Title\nLocation\nPosted X")
                job_title_raw = anchor_text or ""
                job_title = clean_title(job_title_raw)
                job_location = ""
                posting_date = ""

                # some anchors contain newline-separated lines (role, location, posted)
                if "\n" in anchor_text:
                    parts = [p.strip() for p in anchor_text.splitlines() if p.strip()]
                    # heuristics: first part candidate title, last part may be location or posted
                    if parts:
                        if not job_title:
                            job_title = clean_title(parts[0])
                        if len(parts) >= 2:
                            # detect location-like last line
                            if re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta|berlin|paris|singapore)\b', parts[-1], re.I):
                                job_location = parts[-1]
                            # detect posted days
                            m = re.search(r'posted\s+(\d+)\s+days?\s+ago', parts[-1], re.I)
                            if m:
                                posting_date = (date.today() - timedelta(days=int(m.group(1)))).isoformat()

                # attempt to parse common inline location markers in anchor text: " - City, Country" or " | City"
                loc_match = re.search(r'[-|]\s*([A-Za-z \-\,]+)$', anchor_text)
                if loc_match and not job_location:
                    cand = loc_match.group(1).strip()
                    if len(cand) < 80:
                        job_location = cand

                # If critical info missing (location or posting_date), open detail page up to limits
                need_detail = (not job_location or not posting_date) and (detail_opens < MAX_DETAIL_PER_COMPANY) and (attempts_when_missing < MAX_DETAIL_ATTEMPTS_IF_MISSING)
                if need_detail:
                    attempts_when_missing += 1
                    try:
                        # use fast nav with 20s timeout
                        pg_html = fetch_page_content(page, link, nav_timeout=DETAIL_NAV_TIMEOUT, idle_timeout=DETAIL_IDLE_TIMEOUT)
                        detail_opens += 1
                        if pg_html:
                            s = BeautifulSoup(pg_html, "lxml")
                            # prefer H1 if title not good
                            if (not job_title or len(job_title) < 3) and s.find("h1"):
                                job_title = clean_title(s.find("h1").get_text(" ", strip=True))
                            # location selectors common
                            for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--text"]:
                                el = s.select_one(sel)
                                if el and el.get_text(strip=True):
                                    job_location = el.get_text(" ", strip=True)
                                    break
                            # search for text patterns like "Location: City"
                            txt = s.get_text(" ", strip=True)
                            mloc = re.search(r'Location[:\s]*([A-Za-z \-\,]+)', txt, re.I)
                            if mloc and not job_location:
                                job_location = mloc.group(1).strip()
                            # posting date extraction
                            posted = extract_date_from_html(pg_html)
                            if posted:
                                posting_date = posted
                            else:
                                m = re.search(r'posted\s+(\d+)\s+days?\s+ago', txt, re.I)
                                if m:
                                    posting_date = (date.today() - timedelta(days=int(m.group(1)))).isoformat()
                    except Exception as e:
                        print(f"[WARN] detail open failed {link} -> {e}")

                # final cleanup of title
                job_title = clean_title(job_title)
                # remove residual noise
                job_title = re.sub(r'learn more.*', '', job_title, flags=re.I).strip()
                job_title = re.sub(r'apply.*', '', job_title, flags=re.I).strip()

                rows.append({
                    "Company": company,
                    "Job Title": job_title,
                    "Job Link": link,
                    "Location": job_location,
                    "Posting Date": posting_date,
                    "Days Since Posted": days_since(posting_date) if posting_date else ""
                })

        browser.close()

    # dedupe by Job Link (keep first)
    seen_links = set()
    out = []
    for r in rows:
        lk = (r.get("Job Link") or "").strip()
        if not lk or lk in seen_links:
            continue
        seen_links.add(lk)
        out.append(r)

    outfile = "jobs_final_hard.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"])
        writer.writeheader()
        for r in out:
            writer.writerow(r)
    print(f"[OK] wrote {len(out)} rows -> {outfile}")

if __name__ == "__main__":
    try:
        scrape()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
