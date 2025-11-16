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

# ------------------ CONFIG: put your canonical career URLs here ------------------
# Each company -> list of exact career listing URLs you want scraped (ONLY these will be used)
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

# Throttles & timeouts
PAGE_NAV_TIMEOUT = 20000          # ms for listing page goto
PAGE_IDLE_TIMEOUT = 8000          # ms wait for networkidle on listing
DETAIL_NAV_TIMEOUT = 20000        # ms for goto on detail page (but we limit time spent)
DETAIL_PAGE_MAX_SECONDS = 20      # absolute max seconds to attempt detail fetching per job
SLEEP_BETWEEN_REQUESTS = 0.20     # polite delay between requests

# Patterns / helpers
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply', r'view all', r'view openings', r'location', r'locations',
    r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs',
    r'features', r'pricing', r'resources', r'news', r'events'
]
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia)\b', re.I)
JOB_ROLE_WORDS = r'(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|support|ops)'

def clean_title(raw: str) -> str:
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = t.replace("\n", " ").replace("\r", " ")
    # drop language labels and common junk substrings
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply\s*now.*', '', t, flags=re.I)
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = re.sub(r'[\u00A0\u200B]', ' ', t)
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
    m4 = re.search(r'posted[:\s]*([A-Za-z0-9,\- ]+)', html, re.I)
    if m4:
        iso = re.search(r'(\d{4}-\d{2}-\d{2})', m4.group(1))
        if iso:
            return iso.group(1)
    # human "Posted 21 days ago" -> convert to date
    m5 = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m5:
        days = int(m5.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    return ""

def days_since(iso_str: str) -> str:
    if not iso_str:
        return ""
    try:
        d = datetime.fromisoformat(iso_str).date()
    except:
        try:
            d = datetime.strptime(iso_str, "%Y-%m-%d").date()
        except:
            return ""
    return str((date.today() - d).days)

def is_likely_job_anchor(href: str, text: str) -> bool:
    # VERY strict: must contain common job markers or be on known ATS domains
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False
    low_href = (href or "").lower()
    low_text = (text or "").lower()
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'apply', 'greenhouse', 'lever.co', 'boards.greenhouse', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'job-boards', 'job-boards.eu']
    if any(p in low_href for p in positives):
        return True
    if any(p in low_text for p in positives):
        return True
    # heuristic: short title with role word
    if text and 2 <= len(text.split()) <= 8 and re.search(JOB_ROLE_WORDS, text, re.I):
        return True
    return False

def parse_listing_for_links(base_url: str, html: str):
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    # Primary: anchors
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        text = a.get_text(" ", strip=True) or ""
        href_abs = normalize_link(base_url, href)
        if is_likely_job_anchor(href_abs, text):
            # skip if anchor is clearly nav or language toggle
            if re.search(r'\b(privacy|cookie|terms|contact|docs|help|legal|features|pricing)\b', text, re.I):
                continue
            candidates.append((href_abs, text))
    # Secondary: job-card-like elements
    for el in soup.select(".job, .job-listing, .job-card, .opening, .position, .posting, [data-job], .jobItem"):
        a = el.find("a", href=True)
        if a:
            href = normalize_link(base_url, a.get("href"))
            text = a.get_text(" ", strip=True) or el.get_text(" ", strip=True)
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

def fetch_page_content(page, url):
    try:
        page.goto(url, timeout=PAGE_NAV_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=PAGE_IDLE_TIMEOUT)
        return page.content()
    except PWTimeout:
        # fallback to domcontentloaded
        try:
            page.goto(url, timeout=PAGE_NAV_TIMEOUT, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] fallback failed for {url}: {e}")
            return ""
    except Exception as e:
        print(f"[WARN] fetch failed for {url}: {e}")
        return ""

def safe_fetch_detail(page, url):
    """Open detail page but ensure we spend at most DETAIL_PAGE_MAX_SECONDS in total for this job."""
    start = time.time()
    try:
        # convert allowed ms to min(DETAIL_NAV_TIMEOUT, DETAIL_PAGE_MAX_SECONDS*1000)
        allowed_ms = min(DETAIL_NAV_TIMEOUT, int(DETAIL_PAGE_MAX_SECONDS * 1000))
        page.goto(url, timeout=allowed_ms)
        # wait a short time for content, but bounded
        remaining_ms = max(1000, int((DETAIL_PAGE_MAX_SECONDS - (time.time() - start)) * 1000))
        wait_ms = min(3000, remaining_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=wait_ms)
        except:
            pass
        return page.content()
    except PWTimeout:
        print(f"[WARN] detail timeout: {url}")
        return ""
    except Exception as e:
        print(f"[WARN] detail fetch failed: {url} -> {e}")
        return ""

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        for company, urls in COMPANIES.items():
            for url in urls:
                print(f"[SCRAPING] {company} → {url}")
                listing_html = fetch_page_content(page, url)
                if not listing_html:
                    print(f"[WARN] no listing HTML for {company} {url}")
                    continue

                candidates = parse_listing_for_links(url, listing_html)
                # If iframe ATS embed detected, try to fetch their src too
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards")):
                            src_full = normalize_link(url, src)
                            print(f"[INFO] iframe feed found -> {src_full}")
                            html2 = fetch_page_content(page, src_full)
                            if html2:
                                candidates += parse_listing_for_links(src_full, html2)

                # If still empty, best-effort: pick anchors from listing_html
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for a in soup.find_all("a", href=True):
                        href = normalize_link(url, a.get("href"))
                        text = a.get_text(" ", strip=True) or ""
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text))

                # Final filters (remove exact duplicates & listing root)
                final_links = []
                seen = set()
                for link, text in candidates:
                    if not link or link.rstrip("/") == url.rstrip("/"):
                        continue
                    if IMAGE_EXT.search(link):
                        continue
                    # avoid marketing/product pages
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', link, re.I):
                        if not re.search(JOB_ROLE_WORDS, text, re.I):
                            continue
                    if link in seen:
                        continue
                    seen.add(link)
                    final_links.append((link, text))

                # For each candidate, attempt to extract the fields
                for link, anchor_text in final_links:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    raw_title = anchor_text or ""
                    job_title = clean_title(raw_title)
                    job_location = ""
                    posting_date = ""

                    # If anchor text contains newline (common): separate parts
                    if '\n' in anchor_text:
                        lines = [l.strip() for l in anchor_text.splitlines() if l.strip()]
                        # heuristics: last line often location or 'Read more'
                        if len(lines) >= 2:
                            last = lines[-1]
                            if re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta|singapore)\b', last, re.I):
                                job_location = last
                                if not job_title:
                                    job_title = clean_title(lines[0])
                            else:
                                # sometimes second line is dept, third is location
                                for ln in lines[1:]:
                                    if re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta|singapore)\b', ln, re.I):
                                        job_location = ln
                                        break

                    # If title is empty or obviously marketing, try H1 from the detail page
                    need_detail = (not job_title) or (len(job_location) == 0) or (not posting_date)
                    if need_detail:
                        detail_html = safe_fetch_detail(page, link)
                        if detail_html:
                            soup = BeautifulSoup(detail_html, "lxml")
                            # try h1 title
                            if (not job_title or len(job_title) < 3) and soup.find("h1"):
                                job_title = clean_title(soup.find("h1").get_text(" ", strip=True))

                            # try location selectors commonly used
                            loc_candidates = []
                            for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--text"]:
                                el = soup.select_one(sel)
                                if el:
                                    txt = el.get_text(" ", strip=True)
                                    if txt:
                                        loc_candidates.append(txt)
                            # try microdata or structured fields
                            ld = re.search(r'"jobLocation"\s*:\s*{[^}]*"address"\s*:\s*{[^}]*"addressLocality"\s*:\s*"([^"]+)"', detail_html)
                            if ld:
                                loc_candidates.append(ld.group(1))

                            if loc_candidates and not job_location:
                                job_location = loc_candidates[0]

                            # posting date
                            posted = extract_date_from_html(detail_html)
                            if posted:
                                posting_date = posted
                            else:
                                # search for textual "Posted" nearby
                                txt = soup.get_text(" ", strip=True)
                                posted_local = re.search(r'posted\s+(\d+)\s+days?\s+ago', txt, re.I)
                                if posted_local:
                                    posting_date = (date.today() - timedelta(days=int(posted_local.group(1)))).isoformat()

                    # If still no title, fallback to anchor text cleaned
                    job_title = clean_title(job_title or anchor_text or "")

                    # additional cleaning: remove lingering "Learn more & Apply" etc
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
        lk = (r.get("Job Link") or "").strip()
        if not lk:
            continue
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
