# jobs_smart.py
# Hybrid Playwright + BeautifulSoup ATS scraper
# Output -> jobs_final_hard.csv
# Usage:
#   pip install -r requirements.txt
#   python jobs_smart.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys
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

# --- timeouts / throttles ---
PAGE_NAV_TIMEOUT = 45000          # ms for main listing
PAGE_IDLE_TIMEOUT = 30000         # ms for listing networkidle
DETAIL_NAV_TIMEOUT = 20000        # ms per detail page (strict)
DETAIL_IDLE_TIMEOUT = 10000
SLEEP_BETWEEN_REQUESTS = 0.18     # polite

# --- lists and patterns ---
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg|webp)$", re.I)
FORBIDDEN = [
    "privacy", "privacy policy", "company", "about", "about us", "legal", "terms",
    "terms of service", "terms & conditions", "cookie", "cookie policy", "data processing",
    "policies", "resources", "blog", "events", "news", "docs", "documentation", "partners",
    "partner program", "community", "open source", "security", "trust", "whitepapers",
    "customer stories", "product", "pricing", "platform", "solutions", "use cases",
    "comparison", "dashboard", "profile", "account", "contact", "contact us",
    "help", "support", "subscribe", "newsletter", "faq", "language", "careers", "career",
    "view all", "view openings", "home", "locations", "our locations", "company overview",
    "testimonials", "press", "press releases", "sitemap", "investors", "leadership",
    "team", "management", "load more", "featured", "apply now"
]
FORBIDDEN_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in FORBIDDEN) + r')\b', re.I)

# common location tokens (extend as needed)
LOC_TOKENS = [
    "remote", "hybrid", "usa", "united states", "united kingdom", "uk", "germany",
    "france", "canada", "london", "new york", "singapore", "bengaluru", "bangalore",
    "chennai", "berlin", "paris", "india", "atlanta", "georgia", "amsterdam",
    "barcelona", "sao paulo", "brazil", "toronto", "zürich", "zurich", "dublin",
    "stockholm", "oslo", "helsinki", "remote -", "remote,"
]
LOC_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LOC_TOKENS) + r')\b', re.I)

ROLE_WORDS_RE = re.compile(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|engineering|specialist)\b', re.I)

# Helpers
def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    parsed = urlparse(href)
    if parsed.netloc:
        return href
    try:
        return urljoin(base, href)
    except:
        return href

def extract_date_from_html(html_text):
    if not html_text:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        return m.group(1).split("T")[0]
    m2 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
    if m2:
        try:
            return datetime.fromisoformat(m2.group(1)).date().isoformat()
        except:
            return m2.group(1)
    m3 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m3:
        return m3.group(1).split("T")[0]
    # "Posted 21 days ago" -> compute date
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        days = int(mm.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    return ""

def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    # remove bad phrases, languages and 'learn more' style noise
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = FORBIDDEN_RE.sub('', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def extract_location_from_text(txt):
    """
    Heuristic: given a string that may contain role + location,
    attempt to extract a location portion and return (title_without_location, location_str)
    """
    if not txt:
        return "", ""
    s = txt.replace("\r", " ").replace("\n", " ").strip()
    # split heuristics: often "Role  Location" or "Role - Location" or multiline -> last segment is location
    parts = re.split(r'\s{2,}| - | — | – | \| |,|\n', txt)
    parts = [p.strip() for p in parts if p and p.strip()]
    possible_location = ""
    possible_title = s
    if len(parts) >= 2:
        # last part often location if it contains a location token or looks short
        last = parts[-1]
        if LOC_RE.search(last) or re.search(r'^[A-Za-z .,-]{2,40}$', last) and len(last.split()) <= 4:
            possible_location = last
            possible_title = " ".join(parts[:-1])
    # fallback: search inline for location tokens
    if not possible_location:
        m = LOC_RE.search(s)
        if m:
            # take substring around match (up to end)
            idx = m.start()
            candidate = s[idx:].strip(" -,:;")
            # clean candidate (stop at ' - ' or long tail)
            candidate = re.split(r'\s{2,}| - | — | – | \| ', candidate)[0].strip()
            possible_location = candidate
            # remove candidate text from title
            possible_title = s.replace(candidate, '').strip(" -:,")
    # normalize duplicates and commas
    possible_location = normalize_location(possible_location)
    possible_title = re.sub(r'\s{2,}', ' ', possible_title).strip(" -:,")
    return possible_title, possible_location

def normalize_location(loc):
    if not loc:
        return ""
    # catch comma separated duplicates like "Remote, singapore, singapore"
    parts = [p.strip() for p in re.split(r'[,/]+', loc) if p.strip()]
    # dedupe preserving order (case-insensitive)
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            out.append(p.title() if p.lower() != 'remote' else 'Remote')
    return ", ".join(out)

def is_likely_job_anchor(href, text):
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False
    low = (text or href or "").lower()
    if FORBIDDEN_RE.search(low):
        return False
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings', 'greenhouse', 'lever.co', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet', 'gr8people', 'boards.greenhouse', 'job-boards', 'job-boards.eu']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    # heuristic: looks like a role
    if text and ROLE_WORDS_RE.search(text):
        # but avoid very long marketing text
        if len(text) < 120:
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
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text))
    # job-card containers fallback
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role"):
        a = el.find("a", href=True)
        if a:
            href = normalize_link(base_url, a.get("href"))
            text = a.get_text(" ", strip=True) or el.get_text(" ", strip=True)
            if is_likely_job_anchor(href, text):
                candidates.append((href, text))
    # dedupe while preserving order
    seen = set()
    out = []
    for href, text in candidates:
        if not href:
            continue
        # drop same-as-main landing page
        if href.rstrip("/") == base_url.rstrip("/"):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text))
    return out

def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, idle_timeout=PAGE_IDLE_TIMEOUT, wait_dom=False):
    try:
        if wait_dom:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
        else:
            page.goto(url, timeout=nav_timeout)
        page.wait_for_load_state("networkidle", timeout=idle_timeout)
        return page.content()
    except PWTimeout:
        # fallback as best-effort
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] timeout/fetch failed: {url} -> {e}")
            return ""
    except Exception as e:
        print(f"[WARN] fetch failed: {url} -> {e}")
        return ""

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        for company, url_list in COMPANIES.items():
            for main_url in url_list:
                print(f"[SCRAPING] {company} -> {main_url}")
                listing_html = fetch_page_content(page, main_url)
                if not listing_html:
                    print(f"[WARN] no html for {company} ({main_url}) - skipping")
                    continue

                # get candidates from listing HTML
                candidates = parse_listing_for_links(main_url, listing_html)

                # if listing had iframe to ATS, try that
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards")):
                            src_full = normalize_link(main_url, src)
                            print(f"[INFO] found iframe src -> {src_full} (trying)")
                            iframe_html = fetch_page_content(page, src_full)
                            if iframe_html:
                                candidates += parse_listing_for_links(src_full, iframe_html)

                # final lightweight fallback: try any anchors (best-effort)
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for a in soup.find_all("a", href=True):
                        text = a.get_text(" ", strip=True) or ""
                        href = normalize_link(main_url, a.get("href"))
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text))

                # final filter: remove product/docs/marketing pages
                filtered = []
                for href, text in candidates:
                    if IMAGE_EXT.search(href):
                        continue
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                        if not ROLE_WORDS_RE.search(text):
                            continue
                    filtered.append((href, text))

                # For each candidate, try lightweight extraction from anchor text (no detail visit)
                for link, anchor_text in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    # try to parse title & location from anchor text
                    title_candidate = anchor_text or ""
                    title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    if not title_clean:
                        title_clean = clean_title(title_candidate)

                    # posting date best-effort from anchor text
                    posting_date = ""
                    posted_from_anchor = re.search(r'posted\s+(\d+)\s+days?\s+ago', anchor_text or "", re.I)
                    if posted_from_anchor:
                        d = date.today() - timedelta(days=int(posted_from_anchor.group(1)))
                        posting_date = d.isoformat()

                    # If location or posting_date missing, only then open detail page (BUT max 20s)
                    need_detail = (not location_candidate) or (not posting_date)
                    detail_html = ""
                    if need_detail:
                        detail_html = fetch_page_content(page, link, nav_timeout=DETAIL_NAV_TIMEOUT, idle_timeout=DETAIL_IDLE_TIMEOUT)
                        if detail_html:
                            # extract H1 title if title was noisy
                            try:
                                s = BeautifulSoup(detail_html, "lxml")
                                if (not title_clean or len(title_clean) < 3) and s.find("h1"):
                                    title_clean = clean_title(s.find("h1").get_text(" ", strip=True))
                                # location selectors
                                for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--name"]:
                                    el = s.select_one(sel)
                                    if el and el.get_text(strip=True):
                                        location_candidate = el.get_text(" ", strip=True)
                                        break
                                # posting date
                                pd = extract_date_from_html(detail_html)
                                if pd:
                                    posting_date = pd
                                else:
                                    # search textual posted X days ago
                                    txt = s.get_text(" ", strip=True)
                                    pd2 = re.search(r'posted\s+(\d+)\s+days?\s+ago', txt, re.I)
                                    if pd2:
                                        posting_date = (date.today() - timedelta(days=int(pd2.group(1)))).isoformat()
                            except Exception as e:
                                print(f"[WARN] detail parse fail {link} -> {e}")

                    # final normalization
                    title_final = clean_title(title_clean) if title_clean else clean_title(anchor_text)
                    location_final = normalize_location(location_candidate)
                    posting_date_final = posting_date or ""

                    # if title still empty, fallback to anchor text trimmed
                    if not title_final:
                        title_final = clean_title(anchor_text)

                    rows.append({
                        "Company": company,
                        "Job Title": title_final,
                        "Job Link": link,
                        "Location": location_final,
                        "Posting Date": posting_date_final,
                        "Days Since Posted": ""  # filled later
                    })

        browser.close()

    # compute Days Since Posted and dedupe by Job Link
    dedup = {}
    for r in rows:
        lk = r.get("Job Link") or ""
        # keep first occurrence (the earliest added)
        if lk in dedup:
            continue
        # days since posted
        pd = r.get("Posting Date") or ""
        if pd:
            try:
                d = datetime.fromisoformat(pd).date()
                r["Days Since Posted"] = str((date.today() - d).days)
            except:
                r["Days Since Posted"] = ""
        else:
            r["Days Since Posted"] = ""
        dedup[lk] = r

    out = list(dedup.values())

    # sort output by Company then Job Title (Option A)
    out_sorted = sorted(out, key=lambda x: (x.get("Company","").lower(), x.get("Job Title","").lower()))

    outfile = "jobs_final_hard.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"])
        writer.writeheader()
        for r in out_sorted:
            writer.writerow(r)

    print(f"[OK] wrote {len(out_sorted)} rows -> {outfile}")

if __name__ == "__main__":
    try:
        scrape()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
