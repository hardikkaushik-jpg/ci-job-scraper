# jobs_smart.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages (Option A)
# Output -> jobs_final_hard.csv
# Usage:
#   pip install -r requirements.txt
#   python jobs_smart.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json, re, csv, time, sys
from datetime import datetime, date, timedelta

# ------------------ CONFIG: canonical career URLs (use only these) ------------------
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
    "Snowflake": ["https://careers.snowflake.com/global/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"]
}
# ---------------------------------------------------------------------------------

# Timeouts & throttling
PAGE_NAV_TIMEOUT = 40000
PAGE_IDLE_TIMEOUT = 20000
DETAIL_NAV_TIMEOUT = 30000
DETAIL_IDLE_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.20

# Regex and heuristics
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español",
             "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply', r'view all', r'view openings', r'location', r'locations',
    r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs',
    r'features', r'pricing', r'resources', r'news', r'events'
]
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia)\b', re.I)
POSITIVE_LINK_FRAGS = [
    'greenhouse', 'lever.co', 'myworkdayjobs', 'bamboohr', 'jobs', '/job/', '/jobs/', 'open-positions', 'openings', 'ashby', 'comeet', 'gr8people', 'job-boards'
]

# helpers
def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("mailto:") or href.startswith("tel:"):
        return ""
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = t.replace("\n", " ").replace("\r", " ")
    # remove language labels and common junk
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply\s*now.*', '', t, flags=re.I)
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = re.sub(r'[\u00A0\u200B]', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def extract_date_from_html(html):
    # JSON-LD datePosted
    try:
        for js in re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S|re.I):
            try:
                obj = json.loads(js.strip())
            except:
                continue
            # If obj is list, iterate
            items = obj if isinstance(obj, list) else [obj]
            for it in items:
                if isinstance(it, dict) and it.get("@type") in ("JobPosting", "jobPosting", "JobPosting"):
                    dp = it.get("datePosted") or it.get("dateposted")
                    if dp:
                        return dp.split("T")[0]
    except Exception:
        pass

    # meta article:published_time
    m2 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m2:
        v = m2.group(1)
        return v.split("T")[0] if "T" in v else v

    # <time datetime="">
    m3 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m3:
        return m3.group(1).split("T")[0]

    # textual "Posted X days ago" or "Posted on ..."
    m4 = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m4:
        days = int(m4.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    m5 = re.search(r'posted[:\s]*on\s*([A-Za-z0-9,\s-]+)', html, re.I)
    if m5:
        # try to parse date substring
        txt = m5.group(1).strip()
        try:
            parsed = datetime.strptime(txt, "%B %d, %Y").date()
            return parsed.isoformat()
        except:
            pass
    return ""

def extract_location_from_jsonld(html):
    # try to parse JSON-LD jobLocation
    try:
        for js in re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S|re.I):
            try:
                obj = json.loads(js.strip())
            except:
                continue
            items = obj if isinstance(obj, list) else [obj]
            for it in items:
                if not isinstance(it, dict):
                    continue
                # jobLocation can be object or list
                jl = it.get("jobLocation") or it.get("joblocation") or it.get("workLocation")
                if jl:
                    # if object with address
                    if isinstance(jl, dict):
                        # address inside
                        addr = jl.get("address")
                        if isinstance(addr, dict):
                            parts = [addr.get(k,"") for k in ("addressLocality","addressRegion","addressCountry")]
                            joined = ", ".join([p for p in parts if p])
                            if joined:
                                return joined
                        # sometimes it's simple text
                        if "name" in jl and jl.get("name"):
                            return jl.get("name")
                    elif isinstance(jl, list):
                        for entry in jl:
                            if isinstance(entry, dict):
                                addr = entry.get("address")
                                if isinstance(addr, dict):
                                    parts = [addr.get(k,"") for k in ("addressLocality","addressRegion","addressCountry")]
                                    joined = ", ".join([p for p in parts if p])
                                    if joined:
                                        return joined
                                if entry.get("name"):
                                    return entry.get("name")
                    elif isinstance(jl, str):
                        return jl
    except Exception:
        pass
    return ""

def extract_location_from_selectors(soup):
    selectors = [
        "span.location", ".job-location", ".location", ".posting-location",
        ".job_meta_location", "[data-test='job-location']", ".job-location__name"
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # try microdata address
    addr = soup.find(attrs={"itemprop":"jobLocation"})
    if addr:
        return addr.get_text(" ", strip=True)
    # fallback: search for "Location:" pattern in text blocks
    body = soup.get_text(" ", strip=True)
    m = re.search(r'Location[:\s]*([A-Za-z0-9, \-\(\)\/]+)', body, re.I)
    if m:
        return m.group(1).strip()
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
    delta = (date.today() - d).days
    return str(delta)

def is_likely_job_anchor(href, text):
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False
    low_href = (href or "").lower()
    low_text = (text or "").lower()
    # prefer explicit ATS fragments
    if any(fr in low_href for fr in POSITIVE_LINK_FRAGS):
        return True
    # also accept anchors with role-like words
    if re.search(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa)\b', low_text):
        return True
    return False

def parse_listing_for_links(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = (a.get_text(" ", strip=True) or "")
        href_abs = normalize_link(base_url, href)
        if not href_abs:
            continue
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text))
    # also find job-card-like elements
    for el in soup.select(".job, .job-listing, .job-card, .opening, .position, .posting"):
        a = el.find("a", href=True)
        if a:
            href = normalize_link(base_url, a.get("href"))
            text = a.get_text(" ", strip=True) or el.get_text(" ", strip=True)
            if is_likely_job_anchor(href, text):
                candidates.append((href, text))
    # dedupe preserve order
    seen = set()
    out = []
    for href, text in candidates:
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text))
    return out

def fetch_page(page, url, nav_timeout=PAGE_NAV_TIMEOUT, idle_timeout=PAGE_IDLE_TIMEOUT):
    try:
        page.goto(url, timeout=nav_timeout, wait_until="networkidle")
        page.wait_for_load_state("networkidle", timeout=idle_timeout)
        return page.content()
    except PWTimeout:
        # try domcontentloaded fallback
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] fallback load failed for {url}: {e}")
            return ""
    except Exception as e:
        print(f"[WARN] failed to fetch {url}: {e}")
        return ""

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        for company, url_list in COMPANIES.items():
            # url_list might contain multiple team URLs (MongoDB, Precisely)
            for base_url in url_list:
                print(f"[SCRAPING] {company} -> {base_url}")
                html = fetch_page(page, base_url)
                if not html:
                    print(f"[WARN] no html for {company} ({base_url}), skipping")
                    continue

                candidates = parse_listing_for_links(base_url, html)
                # If no candidates, attempt to inspect iframes referencing ATS
                if not candidates:
                    soup = BeautifulSoup(html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = normalize_link(base_url, iframe.get("src"))
                        if src and any(k in src for k in ("greenhouse", "lever.co", "myworkday", "bamboohr", "ashby", "job-boards")):
                            print(f"[INFO] found iframe -> {src}")
                            html2 = fetch_page(page, src)
                            if html2:
                                candidates += parse_listing_for_links(src, html2)

                # final fallback: pick anchors but filter aggressively
                if not candidates:
                    soup = BeautifulSoup(html, "lxml")
                    for a in soup.find_all("a", href=True):
                        href = normalize_link(base_url, a.get("href"))
                        text = a.get_text(" ", strip=True) or ""
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text))

                # filter candidates: avoid landing/product pages
                filtered = []
                for href, text in candidates:
                    # ignore same page root
                    if href.rstrip('/') == base_url.rstrip('/'):
                        continue
                    if IMAGE_EXT.search(href):
                        continue
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                        # if anchor text looks like job title still accept
                        if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
                            continue
                    filtered.append((href, text))

                # iterate detail pages to extract title/location/date
                for link, anchor_text in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    print(f"  [DETAIL] {link}")
                    job_title = clean_title(anchor_text)
                    job_location = ""
                    posting_date = ""

                    # fetch detail page and extract H1/title, location, date from JSON-LD/selectors
                    detail_html = fetch_page(page, link, nav_timeout=DETAIL_NAV_TIMEOUT, idle_timeout=DETAIL_IDLE_TIMEOUT)
                    if detail_html:
                        soup = BeautifulSoup(detail_html, "lxml")
                        # prefer H1 if anchor was noisy
                        if (not job_title or len(job_title) < 3) and soup.find("h1"):
                            job_title = clean_title(soup.find("h1").get_text(" ", strip=True))
                        # JSON-LD location
                        jl_loc = extract_location_from_jsonld(detail_html)
                        if jl_loc:
                            job_location = jl_loc
                        else:
                            # selector-based
                            sel_loc = extract_location_from_selectors(soup)
                            if sel_loc:
                                job_location = sel_loc
                        # posting date
                        pd = extract_date_from_html(detail_html)
                        if pd:
                            posting_date = pd
                        else:
                            # fallback: search text for 'Posted X days ago' etc
                            txt = soup.get_text(" ", strip=True)
                            m = re.search(r'posted\s+(\d+)\s+days?\s+ago', txt, re.I)
                            if m:
                                days = int(m.group(1))
                                posting_date = (date.today() - timedelta(days=days)).isoformat()

                    # final cleanup
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

    # dedupe by Job Link (keep first)
    seen = set()
    out = []
    for r in rows:
        lk = r.get("Job Link") or ""
        if lk in seen:
            continue
        seen.add(lk)
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
