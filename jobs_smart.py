# jobs_smart.py
# Hybrid Playwright + BeautifulSoup scraper for ATS job pages
# Output -> jobs_final_hard.csv
#
# Usage (locally or in GH Actions):
#   pip install -r requirements.txt
#   playwright install chromium
#   python jobs_smart.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys
from datetime import datetime, date, timedelta

# ------------------ CONFIG: canonical career URLs (only these will be used) ------------------
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
# -----------------------------------------------------------------------------------------

# timeouts & throttles
LISTING_NAV_TIMEOUT = 30_000     # ms
LISTING_IDLE_TIMEOUT = 12_000   # ms
DETAIL_NAV_TIMEOUT = 20_000     # ms -> max 20s on detail pages (as requested)
DETAIL_IDLE_TIMEOUT = 8_000     # ms
SLEEP_BETWEEN_REQUESTS = 0.15

# patterns / helpers
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español", "English", "한국어", "简体中文"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
BAD_TITLE_PATTERNS = [r'learn more', r'apply', r'view all', r'view openings', r'location', r'locations', r'career', r'careers', r'profile', r'privacy', r'cookie', r'terms', r'docs', r'features', r'pricing', r'resources', r'news', r'events']
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin|Atlanta|Georgia)\b', re.I)

def clean_title(raw):
    """Return a cleaned job title with location/lang/marketing noise removed (Option C)."""
    if not raw:
        return ""
    t = re.sub(r'[\r\n\t]+', ' ', raw)
    t = re.sub(r'\s+', ' ', t).strip()
    t = LANG_RE.sub('', t)
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    # remove trailing 'Learn More & Apply' style strings
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = re.sub(r'apply\s*now.*', '', t, flags=re.I)
    # remove location tokens that accidentally appear in title
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
    parsed = urlparse(href)
    if parsed.netloc:
        return href
    return urljoin(base, href)

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
    # fallback: "Posted X days ago"
    m4 = re.search(r'posted\s+(\d+)\s+days?\s+ago', html, re.I)
    if m4:
        d = date.today() - timedelta(days=int(m4.group(1)))
        return d.isoformat()
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
    low = (text or href or "").lower()
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'apply', 'greenhouse', 'lever.co', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'job-boards', 'boards.greenhouse']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    if text and 2 <= len(text.split()) <= 9 and re.search(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa)\b', text, re.I):
        return True
    return False

def parse_listing_for_links(base_url, html):
    """Parse listing HTML (BS) and return candidate (abs_link, anchor_text) tuples."""
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = (a.get_text(" ", strip=True) or "")
        href_abs = normalize_link(base_url, href)
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text))
    # job-card style fallback selectors
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .opening-item, .job-item"):
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
        if not href:
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text))
    return out

def fetch_page_content(page, url, nav_timeout_ms=LISTING_NAV_TIMEOUT, idle_timeout_ms=LISTING_IDLE_TIMEOUT):
    """Fetch using Playwright with safe timeouts. Returns HTML string or ''."""
    try:
        page.goto(url, timeout=nav_timeout_ms)
        # prefer networkidle but sometimes sites never reach it - fallback to domcontentloaded if timeout
        try:
            page.wait_for_load_state("networkidle", timeout=idle_timeout_ms)
        except PWTimeout:
            # try domcontentloaded
            try:
                page.wait_for_load_state("domcontentloaded", timeout=5000)
            except PWTimeout:
                pass
        return page.content()
    except PWTimeout:
        print(f"[WARN] timeout loading {url}")
        return ""
    except PWError as e:
        print(f"[WARN] playwright error loading {url} -> {e}")
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

        for company, url_list in COMPANIES.items():
            # url_list may be 1 or many (MongoDB team pages etc)
            for url in url_list:
                print(f"[LISTING] {company} -> {url}")
                listing_html = fetch_page_content(page, url)
                if not listing_html:
                    print(f"[WARN] empty listing HTML for {company} -> {url}")
                    continue

                # extract candidate job links from the listing page
                candidates = parse_listing_for_links(url, listing_html)

                # if listing page contains embedded iframe/ATS widget, try to load iframe/srcs pointing to ATS
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "comeet", "job-boards")):
                            src_full = normalize_link(url, src)
                            print(f"[INFO] found iframe/ATS embed -> {src_full}")
                            iframe_html = fetch_page_content(page, src_full)
                            if iframe_html:
                                candidates += parse_listing_for_links(src_full, iframe_html)

                # final fallback: any anchor on page that looks like a job
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for a in soup.find_all("a", href=True):
                        href = normalize_link(url, a.get("href"))
                        text = a.get_text(" ", strip=True) or ""
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text))

                # dedupe candidate links
                seen = set()
                filtered = []
                for href, text in candidates:
                    if not href:
                        continue
                    if href.rstrip('/') == url.rstrip('/'):
                        continue
                    if IMAGE_EXT.search(href):
                        continue
                    if href in seen:
                        continue
                    seen.add(href)
                    # avoid product/marketing pages unless anchor is clearly a job title
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                        if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', (text or ""), re.I):
                            continue
                    filtered.append((href, text))

                print(f"[INFO] {company} candidates: {len(filtered)}")

                # For each candidate: try to extract title/location/date from anchor text first.
                # Only open detail page if location or posting date missing, and then limit navigation to DETAIL_NAV_TIMEOUT.
                for link, anchor_text in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    # initial parse from anchor text
                    raw_anchor = anchor_text or ""
                    # remove "Learn more & Apply" patterns that sometimes appear inline
                    raw_anchor = re.sub(r'learn\s*more.*', '', raw_anchor, flags=re.I)
                    raw_anchor = re.sub(r'apply\s*now.*', '', raw_anchor, flags=re.I)
                    # attempt to split multi-line anchors (some sites include role \n location \n dept)
                    pieces = [p.strip() for p in raw_anchor.splitlines() if p.strip()]
                    title_guess = ""
                    location_guess = ""
                    posting_guess = ""
                    if pieces:
                        # heuristics: if last piece looks like a location -> assign
                        if len(pieces) >= 2 and re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta|canada|singapore)\b', pieces[-1], re.I):
                            location_guess = pieces[-1]
                            title_guess = pieces[0]
                        else:
                            # often anchor is "Job Title" or "Job Title - Location"
                            # try to split on " - " or " | "
                            t = pieces[0]
                            m = re.split(r'\s[-–|]\s', t)
                            if len(m) >= 2 and re.search(r'\b(remote|hybrid|usa|united|india|germany|france|london|new york|atlanta)\b', m[-1], re.I):
                                title_guess = " - ".join(m[:-1])
                                location_guess = m[-1]
                            else:
                                title_guess = pieces[0]

                    # clean title now
                    job_title_clean = clean_title(title_guess or anchor_text or "")
                    job_location = location_guess or ""
                    job_posting = posting_guess or ""

                    # if we already have both location and posting date from anchor_text -> skip detail page
                    need_detail = not job_location or not job_posting

                    if need_detail:
                        # open detail page but ensure we don't spend > DETAIL_NAV_TIMEOUT
                        try:
                            # navigate with stricter timeout
                            page.goto(link, timeout=DETAIL_NAV_TIMEOUT)
                            try:
                                page.wait_for_load_state("networkidle", timeout=DETAIL_IDLE_TIMEOUT)
                            except PWTimeout:
                                # continue anyway
                                pass
                            detail_html = page.content()
                        except PWTimeout:
                            print(f"[WARN] detail timeout {link}")
                            detail_html = ""
                        except PWError as e:
                            print(f"[WARN] playwright error on detail {link} -> {e}")
                            detail_html = ""
                        except Exception as e:
                            print(f"[WARN] error opening detail {link} -> {e}")
                            detail_html = ""

                        # try extract title (h1), location selectors and date
                        if detail_html:
                            soup = BeautifulSoup(detail_html, "lxml")
                            # H1 candidate
                            h1 = soup.find("h1")
                            if h1 and (not job_title_clean or len(job_title_clean) < 3):
                                job_title_clean = clean_title(h1.get_text(" ", strip=True))
                            # location selectors
                            loc_sel = None
                            for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--text"]:
                                el = soup.select_one(sel)
                                if el and el.get_text(strip=True):
                                    loc_sel = el.get_text(" ", strip=True)
                                    break
                            # Meta reading if present
                            if loc_sel:
                                job_location = loc_sel
                            # try JSON-LD / meta / time tags for posting date
                            d = extract_date_from_html(detail_html)
                            if d:
                                job_posting = d
                            else:
                                # fallback: search near text 'Posted'
                                txt = soup.get_text(" ", strip=True)
                                d2 = re.search(r'posted\s+(\d{4}-\d{2}-\d{2})', txt, re.I)
                                if d2:
                                    job_posting = d2.group(1)

                    # final cleanup: make sure title is present and location is separate
                    job_title_clean = clean_title(job_title_clean)
                    job_location = job_location.strip()
                    job_posting = job_posting.strip()

                    rows.append({
                        "Company": company,
                        "Job Title": job_title_clean,
                        "Job Link": link,
                        "Location": job_location,
                        "Posting Date": job_posting,
                        "Days Since Posted": days_since(job_posting) if job_posting else ""
                    })

        browser.close()

    # dedupe by Job Link (keep first)
    out = []
    seen = set()
    for r in rows:
        lk = (r.get("Job Link") or "").strip()
        if not lk or lk in seen:
            continue
        seen.add(lk)
        out.append(r)

    outfile = "jobs_final_hard.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company", "Job Title", "Job Link", "Location", "Posting Date", "Days Since Posted"])
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
