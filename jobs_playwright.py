# jobs_playwright.py
# Playwright-based ATS-focused job scraper -> produces jobs_tighter.csv
#
# Requirements:
#   pip install -r requirements.txt
#   playwright install  (or workflow will run it)
#
# Behavior:
#   - Renders pages with Playwright (good for JS-heavy career pages)
#   - Filters anchors and job-cards to pick only real job postings
#   - Sanitizes titles (removes language labels, "Learn more", location noise)
#   - Attempts to extract posting date from JSON-LD or common selectors
#   - Writes jobs_tighter.csv (overwrites each run)

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import re, csv, time, sys
from urllib.parse import urljoin, urlparse
from datetime import datetime

# --- CONFIG: paste your 56+ career URLs below (company: url) ---
COMPANIES = {
    "Airtable": "https://airtable.com/careers",
    "Alation": "https://www.alation.com/careers/all-careers/",
    "Alex Solutions": "https://alexsolutions.com/careers",
    "Alteryx": "https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers",
    "Amazon (AWS)": "https://www.amazon.jobs/en/teams/aws",
    "Ataccama": "https://jobs.ataccama.com/",
    "Atlan": "https://atlan.com/careers",
    "Anomalo": "https://www.anomalo.com/careers",
    "BigEye": "https://www.bigeye.com/careers",
    "Boomi": "https://boomi.com/company/careers/",
    "CastorDoc (Coalesce)": "https://coalesce.io/careers/",
    "Cloudera": "https://www.cloudera.com/careers.html",
    "Collibra": "https://www.collibra.com/company/careers",
    "Couchbase": "https://www.couchbase.com/careers/",
    "Data.World (ServiceNow)": "https://data.world/company/careers",
    "Databricks": "https://databricks.com/company/careers/open-positions",
    "Datadog": "https://careers.datadoghq.com/",
    "DataGalaxy": "https://www.welcometothejungle.com/en/companies/datagalaxy/jobs",
    "Decube": "https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41",
    "Exasol": "https://careers.exasol.com/",
    "Firebolt": "https://www.firebolt.io/careers",
    "Fivetran": "https://fivetran.com/careers",
    "GoldenSource": "https://www.thegoldensource.com/careers/",
    "Google (General)": None,  # removed by request
    "IBM": None,               # removed by request
    "InfluxData": "https://www.influxdata.com/careers/",
    "Informatica": "https://informatica.gr8people.com/jobs",
    "MariaDB": "https://mariadb.com/about/careers/",
    "Matillion": "https://www.matillion.com/careers",
    "Microsoft": None,         # removed by request
    "MongoDB (Engineering)": "https://www.mongodb.com/company/careers/teams/engineering",
    "Monte Carlo": "https://jobs.ashbyhq.com/montecarlodata",
    "Mulesoft": "https://www.mulesoft.com/careers",
    "Nutanix": "https://careers.nutanix.com/en/jobs/",
    "OneTrust": "https://www.onetrust.com/careers/",
    "Oracle": "https://careers.oracle.com/en/sites/jobsearch/jobs",
    "Panoply": "https://sqream.com/careers/",
    "PostgreSQL": "https://www.postgresql.org/about/careers/",
    "Precisely (US)": "https://www.precisely.com/careers-and-culture/us-jobs",
    "Qlik": "http://careerhub.qlik.com/careers",
    "SAP": "https://jobs.sap.com/",
    "Sifflet": "https://www.welcometothejungle.com/en/companies/sifflet/jobs",
    "SnapLogic": "https://www.snaplogic.com/company/careers",
    "Snowflake": "https://careers.snowflake.com/",
    "Solidatus": "https://solidatus.bamboohr.com/",
    "SQLite": "https://www.sqlite.org/careers.html",
    "Syniti": "https://careers.syniti.com/",
    "Tencent Cloud": "https://careers.tencent.com/en-us/search.html",
    "Teradata": "https://careers.teradata.com/jobs",
    "Yellowbrick": "https://yellowbrick.com/careers/",
    "Vertica": "https://careers.opentext.com/us/en",
    "Pentaho": "https://www.hitachivantara.com/en-us/company/careers/job-search"
}

# sanitize companies dict: remove None entries (explicitly excluded)
COMPANIES = {k: v for k, v in COMPANIES.items() if v}

# --- helper regex / lists ---
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg)$", re.I)
BAD_TITLE_PATTERNS = [
    r'learn more', r'apply', r'view all', r'product', r'solutions?', r'connectors?',
    r'platform', r'privacy', r'cookie', r'legal', r'contact', r'help', r'docs',
    r'enterprise', r'features', r'pricing', r'resources', r'news', r'events'
]
LANG_TAGS = ["Deutsch", "Français", "Italiano", "日本語", "Português", "Español",
             "English", "한국어", "简体中文", "Deutsch (Deutschland)", "Português"]
LANG_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LANG_TAGS) + r')\b', re.I)
LOCATION_IN_TITLE_RE = re.compile(r'\b(USA|United States|United Kingdom|UK|Remote|Hybrid|Worldwide|India|Germany|France|Canada|London|NY|New York|Singapore|Bengaluru|Chennai|Paris|Berlin)\b', re.I)
APPLY_IN_RE = re.compile(r'apply\s+in', re.I)
EXTRA_CLEAN_RE = re.compile(r'[\u00A0\u200B]+')  # weird spaces

def clean_title(raw):
    if not raw:
        return ""
    t = raw.strip()
    # remove "Learn more & Apply" and similar phrases
    t = re.sub(r'learn more.*$', '', t, flags=re.I)
    t = re.sub(r'learn more\s*&?\s*apply.*$', '', t, flags=re.I)
    t = re.sub(r'view all.*$', '', t, flags=re.I)
    t = re.sub(r'\b(apply|apply now|learn more|learn more & apply)\b', '', t, flags=re.I)
    # remove language labels, "English", "Deutsch", etc.
    t = LANG_RE.sub('', t)
    # remove obvious product/marketing words
    for p in BAD_TITLE_PATTERNS:
        t = re.sub(p, '', t, flags=re.I)
    # remove duplicated location tokens inside title
    t = LOCATION_IN_TITLE_RE.sub('', t)
    t = APPLY_IN_RE.sub('', t)
    t = EXTRA_CLEAN_RE.sub(' ', t)
    # collapse whitespace
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t

def is_likely_job_link(href, text):
    if not href or IMAGE_EXT.search(href):
        return False
    low = (text or href).lower()
    # positive heuristics: common ATS path fragments
    positives = ['/jobs/', '/job/', '/careers/', '/careers/', '/positions/', '/open-positions', '/openings', '/apply/','boards.greenhouse','lever.co','workday','bamboohr','ashby','comeet','gr8people','jobs.','myworkdayjobs','job-boards']
    if any(p in href.lower() for p in positives) or any(p in low for p in positives):
        return True
    # also accept anchors that look like job titles (short, capitalized, has role-like words)
    if 3 <= len(text.split()) <= 7 and re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
        return True
    return False

def extract_posting_date_from_html(page_content):
    # try to find JSON-LD datePosted, or meta[property="article:published_time"], or <time datetime=...>
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', page_content)
    if m:
        return m.group(1).split('T')[0]
    m2 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', page_content, re.I)
    if m2:
        try:
            return datetime.fromisoformat(m2.group(1)).date().isoformat()
        except:
            return m2.group(1)
    m3 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', page_content, re.I)
    if m3:
        return m3.group(1).split('T')[0]
    return ""

def normalize_link(base, href):
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if urlparse(href).netloc:
        return href
    return urljoin(base, href)

def dedupe_keep_latest(rows):
    # simple dedupe by Job Link, keep first occurrence
    seen = set()
    out = []
    for r in rows:
        link = r.get("Job Link","")
        if link in seen:
            continue
        seen.add(link)
        out.append(r)
    return out

def scrape():
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        for company, url in COMPANIES.items():
            print(f"[INFO] scraping {company} -> {url}")
            try:
                page.goto(url, timeout=40000)
                page.wait_for_load_state("networkidle", timeout=20000)
            except PWTimeout:
                print(f"[WARN] timeout loading {url} (continuing)")

            # collect candidate anchors and job-card elements
            anchors = page.query_selector_all("a[href]")
            candidate_links = []
            for a in anchors:
                href = a.get_attribute("href") or ""
                text = (a.inner_text() or "").strip()
                full = normalize_link(url, href)
                if not full:
                    continue
                if is_likely_job_link(full, text):
                    # avoid marketing pages or repeated nav anchors
                    if re.search(r'\b(privacy|cookie|terms|contact|docs|help|legal|resources|product|features|pricing)\b', text, re.I):
                        continue
                    candidate_links.append((full, text))

            # also try to capture job-card anchors (some sites use <div role="link"> or buttons)
            # find elements that look like job cards
            job_cards = page.query_selector_all("a,button,[role='link']")
            for jc in job_cards:
                try:
                    href = jc.get_attribute("href") or ""
                except:
                    href = ""
                text = (jc.inner_text() or "").strip()
                full = normalize_link(url, href) if href else ""
                if full and is_likely_job_link(full, text):
                    candidate_links.append((full, text))

            # make unique while preserving order
            seen = set()
            filtered = []
            for link, text in candidate_links:
                if link in seen:
                    continue
                seen.add(link)
                filtered.append((link, text))

            # If nothing found using anchor heuristics, try to detect job-list items (fallback)
            if not filtered:
                # common job-list selectors used by ATS templates
                hit = page.query_selector_all("li.jb, li.job, div.job, div.opening, .job-listing, .position, .job-card, .open-role")
                for el in hit:
                    try:
                        a = el.query_selector("a[href]")
                        if a:
                            href = a.get_attribute("href") or ""
                            text = (a.inner_text() or el.inner_text() or "").strip()
                            full = normalize_link(url, href)
                            if full and is_likely_job_link(full, text):
                                if full not in seen:
                                    filtered.append((full, text))
                                    seen.add(full)
                    except:
                        pass

            # final cleaning: remove obvious non-job pages
            final_links = []
            for link, text in filtered:
                if IMAGE_EXT.search(link):
                    continue
                # exclude links that are the same as the careers landing / homepage
                if link.rstrip('/') == url.rstrip('/'):
                    continue
                # ignore obvious product pages
                if re.search(r'/product|/features|/pricing|/solutions|/docs|/resources|/legal|/contact', link, re.I):
                    # still accept if the anchor text is clearly a job title
                    if not re.search(r'\b(engineer|manager|analyst|developer|scientist|architect|director|product|sales|success|consultant|designer|qa|sre)\b', text, re.I):
                        continue
                final_links.append((link, text))

            # fetch each job detail page to get cleaned title and posting date (if possible)
            for link, title_text in final_links:
                try:
                    # load detail page
                    page.goto(link, timeout=30000)
                    page.wait_for_load_state("networkidle", timeout=20000)
                    html = page.content()
                except PWTimeout:
                    html = ""
                cleaned_title = clean_title(title_text)
                # fallback: if cleaned_title is empty or looks like nav, try to extract h1/h2 from detail page
                if (not cleaned_title) and html:
                    try:
                        h1 = page.query_selector("h1")
                        if h1 and h1.inner_text().strip():
                            cleaned_title = clean_title(h1.inner_text().strip())
                    except:
                        pass
                # posting date
                posted = extract_posting_date_from_html(html or "")
                # location attempt: find patterns on job detail page
                loc = ""
                try:
                    # try common selectors
                    cand = page.query_selector("span.location, .job-location, .location, [data-test='job-location']")
                    if cand:
                        loc = cand.inner_text().strip()
                    else:
                        # try JSON-LD location or meta
                        m = re.search(r'"hiringOrganization".*', html or "")
                        # fallback blank
                except:
                    loc = ""
                rows.append({
                    "Company": company,
                    "Job Title": cleaned_title or title_text,
                    "Job Link": link,
                    "Location": loc,
                    "Posting Date": posted
                })
                # be polite to sites
                time.sleep(0.25)

        browser.close()

    # dedupe and write CSV
    rows = dedupe_keep_latest(rows)
    out = "jobs_tighter.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company","Job Title","Job Link","Location","Posting Date"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"[OK] wrote {len(rows)} rows -> {out}")

if __name__ == "__main__":
    scrape()
