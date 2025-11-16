# jobs_smart.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages
# Output -> jobs_final_hard.csv
# Usage:
#   pip install -r requirements.txt
#   python jobs_smart.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys, math
from datetime import datetime, timezone, date

# ------------------ CONFIG: put your canonical career URLs here ------------------
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
    "Snowflake": ["https://careers.snowflake.com/us/en/search-results"],
    "Solidatus": ["https://solidatus.bamboohr.com/jobs"],
    "Syniti": ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Tencent Cloud": ["https://careers.tencent.com/en-us/search.html"],
    "Teradata": ["https://careers.teradata.com/jobs"],
    "Yellowbrick": ["https://yellowbrick.com/careers/#positions"],
    "Vertica": ["https://careers.opentext.com/us/en/home"],
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search"],
}
# ---------------------------------------------------------------------------------

# throttles
PAGE_NAV_TIMEOUT = 45000
PAGE_IDLE_TIMEOUT = 30000
DETAIL_NAV_TIMEOUT = 30000
DETAIL_IDLE_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.2

# patterns
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español",
             "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply', r'view all', r'view openings', r'location', r'locations',
    r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs',
    r'features', r'pricing', r'resources', r'news', r'events'
]
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia|United States - Flex)\b', re.I)

def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = t.replace("\n", " ").replace("\r", " ")
    # remove language labels and trailing marketing text
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    # remove "Learn More & Apply" etc
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply\s*now.*', '', t, flags=re.I)
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = re.sub(r'[\u00A0\u200B]', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def extract_date_from_text(s):
    if not s:
        return ""
    # look for ISO date
    iso = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if iso:
        return iso.group(1)
    # look for human 'Posted 21 days ago'
    m = re.search(r'posted\s+(\d+)\s+days?\s+ago', s, re.I)
    if m:
        days = int(m.group(1))
        d = date.today() - timedelta(days=days)
        return d.isoformat()
    # check e.g. "Posted 21 days ago" with 'hours' or 'days'
    m2 = re.search(r'posted\s+(\d+)\s+hours?\s+ago', s, re.I)
    if m2:
        return date.today().isoformat()
    return ""

def extract_date_from_html(html):
    if not html:
        return ""
    # json-ld "datePosted"
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1).split("T")[0]
    # meta article:published_time
    m2 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m2:
        try:
            return datetime.fromisoformat(m2.group(1)).date().isoformat()
        except:
            return m2.group(1)
    # <time datetime="">
    m3 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html, re.I)
    if m3:
        return m3.group(1).split("T")[0]
    # textual "Posted <date>"
    m4 = re.search(r'posted[:\s]*([A-Za-z0-9 ,\-]+)', html, re.I)
    if m4:
        # try iso inside
        iso = re.search(r'(\d{4}-\d{2}-\d{2})', m4.group(1))
        if iso:
            return iso.group(1)
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
    # ignore images
    if href and IMAGE_EXT.search(href):
        return False
    lower = (text or href or "").lower()
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'apply', 'greenhouse', 'lever.co', 'boards.greenhouse', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'job-boards', 'job-boards.eu', 'job-boards']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in lower for p in positives):
        return True
    # heuristics: short title containing role words
    if text and 2 <= len(text.split()) <= 8 and re.search(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa)\b', text, re.I):
        return True
    return False

# parse job list using BS
def parse_listing_for_links(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = (a.get_text(" ", strip=True) or "")
        href_abs = normalize_link(base_url, href)
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text))
    # try specific job-card containers that sometimes are buttons or divs with role=link
    # search for elements with data-job or class containing 'job'
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting"):
        # attempt to find an anchor inside
        a = el.find("a", href=True)
        if a:
            href = normalize_link(base_url, a.get("href"))
            text = (a.get_text(" ", strip=True) or el.get_text(" ", strip=True))
            if is_likely_job_anchor(href, text):
                candidates.append((href, text))
    # dedupe preserving order
    seen = set()
    out = []
    for href, text in candidates:
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text))
    return out

def fetch_page_content(page, url):
    try:
        page.goto(url, timeout=PAGE_NAV_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=PAGE_IDLE_TIMEOUT)
        html = page.content()
        return html
    except PWTimeout:
        print(f"[WARN] timeout loading {url}")
        try:
            # fallback: try domcontentloaded
            page.goto(url, timeout=PAGE_NAV_TIMEOUT, wait_until="domcontentloaded")
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
        for company, url in COMPANIES.items():
            print(f"[SCRAPING] {company} -> {url}")
            html = fetch_page_content(page, url)
            if not html:
                print(f"[WARN] no html for {company} ({url}) -- skipping")
                continue

            # parse candidate job links from listing HTML (BeautifulSoup)
            candidates = parse_listing_for_links(url, html)
            if not candidates:
                # fallback: try some common anchors with JS snippets
                soup = BeautifulSoup(html, "lxml")
                # sometimes jobs are embedded through iframes or script tags referencing an ATS domain
                for iframe in soup.find_all("iframe", src=True):
                    src = iframe.get("src")
                    if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards")):
                        src_full = normalize_link(url, src)
                        print(f"[INFO] found iframe src -> {src_full} (trying)")
                        html2 = fetch_page_content(page, src_full)
                        if html2:
                            candidates += parse_listing_for_links(src_full, html2)

            # if still nothing, capture any obvious anchors on page (best-effort)
            if not candidates:
                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    text = a.get_text(" ", strip=True) or ""
                    href = normalize_link(url, a.get("href"))
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text))

            # final filter: remove links that are just the careers landing page or site root
            filtered = []
            for href, text in candidates:
                if href.rstrip("/") == url.rstrip("/"):
                    continue
                if IMAGE_EXT.search(href):
                    continue
                # avoid obvious marketing pages
                if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                    # allow if anchor text clearly a job title
                    if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
                        continue
                filtered.append((href, text))

            # visit each candidate detail page to get final title/location/posting date
            for link, anchor_text in filtered:
                # polite sleep
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                job_title = clean_title(anchor_text)
                job_location = ""
                posting_date = ""

                # Try to extract some info directly from anchor_text (many listings include location/date)
                if not job_title:
                    job_title = clean_title(anchor_text)
                # If anchor_text contains newline-separated role and location (common on some sites)
                if '\n' in anchor_text and not job_location:
                    pieces = [p.strip() for p in anchor_text.splitlines() if p.strip()]
                    # heuristic: last line often location or dept
                    if len(pieces) >= 2 and re.search(r'\b(?:remote|hybrid|usa|united|india|germany|france|london|new york|atlanta)\b', pieces[-1], re.I):
                        job_location = pieces[-1]
                        if not job_title:
                            job_title = clean_title(pieces[0])

                # Fetch detail page to try to get h1, date, location
                detail_html = fetch_page_content(page, link)
                if detail_html:
                    soup = BeautifulSoup(detail_html, "lxml")
                    # prefer H1 as title if anchor text was noisy
                    if (not job_title or len(job_title) < 3) and soup.find("h1"):
                        job_title = clean_title(soup.find("h1").get_text(" ", strip=True))

                    # try common location selectors
                    loc_sel = None
                    for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location"]:
                        el = soup.select_one(sel)
                        if el and el.get_text(strip=True):
                            loc_sel = el.get_text(" ", strip=True)
                            break
                    if loc_sel:
                        job_location = loc_sel

                    # try to find time/meta
                    posted = extract_date_from_html(detail_html)
                    if posted:
                        posting_date = posted
                    else:
                        # try nearby "posted" strings in text
                        txt = soup.get_text(" ", strip=True)
                        posted_local = extract_date_from_text(txt)
                        if posted_local:
                            posting_date = posted_local

                # final cleanup of title
                job_title = clean_title(job_title)

                # remove residual "Learn More" / "Apply" or language tags in the title
                job_title = re.sub(r'learn more.*', '', job_title, flags=re.I).strip()
                job_title = re.sub(r'apply.*', '', job_title, flags=re.I).strip()
                job_title = LANG_RE.sub('', job_title).strip()
                job_title = re.sub(r'\s{2,}', ' ', job_title)

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
