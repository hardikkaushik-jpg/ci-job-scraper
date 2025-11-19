# jobs_smart_cplus_patched.py
# Playwright + BeautifulSoup hybrid scraper, ATS-aware, safe detail quota.
# All patches applied: Date (Patch1), Location (Patch2), Title+Seniority (Patch3)
# Run with: python3 jobs_smart_cplus_patched.py

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys, json
from datetime import datetime, date, timedelta

# ---------- CONFIG ----------
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

# Timeouts (ms)
PAGE_NAV_TIMEOUT = 40000
PAGE_DOM_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.18
MAX_DETAIL_PAGES = 9999

# Patterns
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg|webp)$", re.I)
FORBIDDEN_WORDS = [
    "privacy","privacy policy","about","legal","terms","cookie","cookies",
    "press","blog","partners","pricing","docs","documentation","support",
    "events","resources","login","signin","register","apply now","careers home"
]
FORBIDDEN_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in FORBIDDEN_WORDS) + r')\b', re.I)

LOC_TOKENS = [
    "remote","hybrid","usa","united states","uk","united kingdom","germany","france","canada",
    "london","new york","singapore","bengaluru","bangalore","chennai","berlin","paris","india",
    "amsterdam","toronto","zurich","dublin","stockholm","oslo","helsinki","australia","brazil"
]
LOC_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LOC_TOKENS) + r')\b', re.I)

ROLE_WORDS_RE = re.compile(
    r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|specialist)\b',
    re.I
)

COMPANY_SKIP_RULES = {
    "Ataccama": [r'one-team', r'platform', r'about'],
    "Fivetran": [r'launchers', r'product', r'developers'],
    "Datadog": [r'resources', r'events', r'product'],
}

CRITICAL_COMPANIES = ["fivetran","ataccama","datadog","snowflake","matillion","oracle"]

# ---------- HELPERS ----------
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


def clean_title(raw):
    """
    Patch 3: Title normalizer / cleaner.
    - Remove common "at Company" fragments and trailing location in parentheses
    - Remove CTA fragments like 'Apply now', 'Learn more'
    - Collapse whitespace, strip punctuation
    - Keep titles short and meaningful
    """
    if not raw:
        return ""
    t = raw.strip()

    # some common separators that split title/location or extraneous info
    # remove any leading/trailing punctuation or separators
    t = re.sub(r'[\r\n\t]+', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t)

    # Remove "at Company" patterns: "Software Engineer at Acme" -> "Software Engineer"
    t = re.sub(r'\s+at\s+[A-Z][\w\-\s&\.]{1,50}$', '', t, flags=re.I)

    # Remove "(Location)" trailing parenthesis blocks that are likely location-only
    t = re.sub(r'\s*\([^)]{1,60}\)\s*$', '', t)

    # Remove trailing location tokens like ' - Remote' or ' — London'
    t = re.sub(r'[\-\—\–]\s*(remote|hybrid|[A-Za-z][\w\s\-]{1,40})\s*$', '', t, flags=re.I)

    # Remove CTA or boilerplate phrases
    t = re.sub(r'(?i)\b(apply now|learn more|see more|view details|read more|more info)\b.*', '', t)

    # Remove excessive punctuation and collapse spaces
    t = re.sub(r'[|••­•·]+', ' ', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    # Strip leftover leading/trailing punctuation
    t = t.strip(" -:,.")
    return t


def normalize_location(loc):
    if not loc:
        return ""
    s = re.sub(r'[\n\r\t]', ' ', loc)
    parts = [p.strip() for p in re.split(r'[,/;|]+', s) if p.strip()]
    seen = set()
    out = []
    for p in parts:
        if p.lower() in seen:
            continue
        seen.add(p.lower())
        if p.lower() == "remote":
            out.append("Remote")
        else:
            out.append(p.title())
    return ", ".join(out)

def _iso_only_date(s):
    try:
        return datetime.fromisoformat(s.split("T")[0]).date().isoformat()
    except:
        try:
            return datetime.strptime(s.split("T")[0], "%Y-%m-%d").date().isoformat()
        except:
            return s.split("T")[0]


def extract_date_from_html(html_text):
    """
    Patch 1: Improved date extractor.
    Tries JSON-LD, known ATS structures, 'posted X days ago', multiple regex windows,
    and robust ISO searches with expanded context window.
    """
    if not html_text:
        return ""

    # 1) JSON-LD common date keys
    # prefer "datePosted" if available
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        return _iso_only_date(m.group(1))

    # common workday/postedOn
    m2 = re.search(r'"postedOn"\s*:\s*"([^"]+)"', html_text)
    if m2:
        return _iso_only_date(m2.group(1))

    # meta article published_time
    m3 = re.search(
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        re.I,
    )
    if m3:
        return _iso_only_date(m3.group(1))

    # <time datetime="...">
    m4 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m4:
        return _iso_only_date(m4.group(1))

    # posted X days ago
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        days = int(mm.group(1))
        return (date.today() - timedelta(days=days)).isoformat()

    # Try NextJS / __NEXT_DATA__ early, or other embedded JSON
    m_next = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', html_text, re.S)
    if m_next:
        try:
            nd = json.loads(m_next.group(1))
            # search for iso-like strings in the json object
            def find_iso(o):
                if isinstance(o, str):
                    if re.match(r'\d{4}-\d{2}-\d{2}', o):
                        return o
                    return None
                if isinstance(o, dict):
                    for v in o.values():
                        res = find_iso(v)
                        if res:
                            return res
                if isinstance(o, list):
                    for it in o:
                        res = find_iso(it)
                        if res:
                            return res
                return None
            res = find_iso(nd)
            if res:
                return _iso_only_date(res)
        except:
            pass

    # Broader scan: look for ISO dates near date keywords in a larger window
    snippet = html_text[:120000]  # larger window
    m_strict = re.search(r'(?i)(posted|created|updated|date|published|posted on|posted at|published on)[^0-9A-Za-z]{0,80}(\d{4}-\d{2}-\d{2})', snippet)
    if m_strict:
        return _iso_only_date(m_strict.group(2))

    # fallback: find first ISO in page
    mm2 = re.search(r'(\d{4}-\d{2}-\d{2})', html_text)
    if mm2:
        return mm2.group(1)

    return ""


def is_likely_job_anchor(href, text):
    if not href and not text:
        return False
    if href and IMAGE_EXT.search(href):
        return False

    low = (text or href or "").lower()

    if FORBIDDEN_RE.search(low):
        return False

    positives = [
        "jobs",
        "/job/",
        "/jobs/",
        "careers",
        "open-positions",
        "openings",
        "greenhouse",
        "lever.co",
        "myworkdayjobs",
        "bamboohr",
        "ashby",
        "comeet",
        "gr8people",
        "boards.greenhouse",
        "/job/",
    ]

    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    if text and ROLE_WORDS_RE.search(text) and len(text) < 200:
        return True

    return False


def extract_location_from_text(txt):
    if not txt:
        return "", ""
    s = txt.replace("\r", " ").replace("\n", " ").strip()

    paren = re.search(r"\(([^)]+)\)\s*$", s)
    if paren:
        loc = paren.group(1)
        title = s[: paren.start()].strip(" -:,")
        return title, normalize_location(loc)

    parts = re.split(r"\s{2,}| - | — | – | \| |·|•| — |,", s)
    parts = [p.strip() for p in parts if p.strip()]

    if len(parts) >= 2:
        last = parts[-1]
        if LOC_RE.search(last) or (len(last.split()) <= 4 and re.search(r"[A-Za-z]", last)):
            return " ".join(parts[:-1]), normalize_location(last)

    m = LOC_RE.search(s)
    if m:
        idx = m.start()
        candidate = s[idx:].strip(" -,:;")
        candidate = re.split(r"\s{2,}| - | — | – | \| ", candidate)[0].strip()
        title = s.replace(candidate, "").strip(" -:,")
        return title, normalize_location(candidate)

    return s, ""


def try_extract_location_from_card(el):
    if not el:
        return ""

    search_selectors = [
        ".location",
        ".job-location",
        "span[class*='location']",
        ".posting-location",
        ".job_meta_location",
        ".location--name",
        ".job-card__location",
        ".opening__meta",
        ".job-meta__location",
        ".opening__location",
        ".jobCard-location",
        ".locationTag",
    ]

    def attr_location(tag):
        if not tag:
            return ""
        for attr in (
            "data-location",
            "data-geo",
            "aria-label",
            "title",
            "data-qa-location",
            "data-test-location",
        ):
            v = tag.get(attr)
            if v and isinstance(v, str) and v.strip():
                return normalize_location(v.strip())
        return ""

    v = attr_location(el)
    if v:
        return v

    for sel in search_selectors:
        try:
            found = el.select_one(sel)
        except Exception:
            found = None
        if found and found.get_text(strip=True):
            return normalize_location(found.get_text(" ", strip=True))

    parent = el.parent
    depth = 0

    while parent and depth < 8:
        v = attr_location(parent)
        if v:
            return v

        for sel in search_selectors:
            try:
                found = parent.select_one(sel)
            except Exception:
                found = None
            if found and found.get_text(strip=True):
                return normalize_location(found.get_text(" ", strip=True))

        for sib in parent.find_all(recursive=False):
            try:
                txt = sib.get_text(" ", strip=True)
            except Exception:
                txt = ""
            if txt and LOC_RE.search(txt):
                if len(txt.split()) <= 6:
                    return normalize_location(txt)

        parent = parent.parent
        depth += 1

    try:
        nearby_text = el.get_text(" ", strip=True)
        if nearby_text and LOC_RE.search(nearby_text):
            cand = re.split(r"\s{2,}| - | \| |—|–|,", nearby_text)[-1].strip()
            if len(cand.split()) <= 6:
                return normalize_location(cand)
    except Exception:
        pass

    return ""


def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, dom_timeout=PAGE_DOM_TIMEOUT):
    try:
        page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
        page.wait_for_timeout(300)
        return page.content()
    except PWTimeout:
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] fetch failed (timeout): {url} -> {e}")
            return ""
    except Exception as e:
        print(f"[WARN] fetch failed: {url} -> {e}")
        return ""

# --- SENIORITY CLASSIFIER (Patch 3: expanded)
def detect_seniority(title):
    if not title:
        return "Unknown"
    t = title.lower()

    # Director / Executive / Leadership
    if any(x in t for x in [
        "chief ", "cxo", "cto", "ceo", "cfo", "coo",
        "vp ", "vice president", "svp", "evp",
        "executive director", "executive", "head of",
        "director", "global director", "managing director"
    ]):
        return "Director+"

    # Principal / Staff / Distinguished
    if any(x in t for x in ["principal", "staff ", "distinguished", "fellow"]):
        return "Principal/Staff"

    # Senior keywords (cover many variants)
    if any(re.search(r'\b' + re.escape(k) + r'\b', t) for k in [
        "senior", "sr\\.", "sr ", "lead", "lead-", "team lead", "senior engineer", "principal engineer"
    ]):
        return "Senior"

    # Manager
    if any(x in t for x in ["manager", "mgr", "management", "people manager"]):
        return "Manager"

    # Mid / Associate
    if any(re.search(r'\b' + re.escape(k) + r'\b', t) for k in [
        "associate", "mid ", "mid-", "intermediate", "ii", "iii", "level 2", "level ii"
    ]):
        return "Mid"

    # Entry / Junior
    if any(re.search(r'\b' + re.escape(k) + r'\b', t) for k in [
        "junior", "jr\\.", "jr ", "entry", "graduate", "fresher", "trainee"
    ]):
        return "Entry"

    # Intern / Working student
    if any(x in t for x in ["intern", "internship", "working student", "werkstudent"]):
        return "Intern"

    # Fallback heuristics: numeric seniority like '3+ years' or '5+ years' often mid or senior
    m_years = re.search(r'(\d+)\+?\s+years', t)
    if m_years:
        yrs = int(m_years.group(1))
        if yrs >= 7:
            return "Senior"
        if yrs >= 3:
            return "Mid"
        return "Entry"

    return "Unknown"

# ---------- MAIN SCRAPE ----------
def scrape():
    rows = []
    detail_count = 0

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

                soup = BeautifulSoup(listing_html, "lxml")
                candidates = []

                # standard anchors
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    text = a.get_text(" ", strip=True) or ""
                    href_abs = normalize_link(main_url, href)
                    if is_likely_job_anchor(href_abs, text):
                        candidates.append((href_abs, text, a))

                # job-card containers
                for el in soup.select(
                    "[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"
                ):
                    a = el.find("a", href=True)
                    text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text, el))

                # try iframe to ATS
                if not candidates:
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(
                            k in src
                            for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "jobs.lever")
                        ):
                            src_full = normalize_link(main_url, src)
                            iframe_html = fetch_page_content(page, src_full)
                            if iframe_html:
                                f_soup = BeautifulSoup(iframe_html, "lxml")
                                for a in f_soup.find_all("a", href=True):
                                    href = a.get("href")
                                    text = a.get_text(" ", strip=True) or ""
                                    href_abs = normalize_link(src_full, href)
                                    if is_likely_job_anchor(href_abs, text):
                                        candidates.append((href_abs, text, a))

                # dedupe & skip rules
                seen = set()
                filtered = []

                for href, text, el in candidates:
                    if not href or href.rstrip("/") == main_url.rstrip("/"):
                        continue
                    if href in seen:
                        continue
                    seen.add(href)

                    skip = False
                    low_text = (text or "").lower()

                    for c, rules in COMPANY_SKIP_RULES.items():
                        if c.lower() == company.lower():
                            for r in rules:
                                if re.search(r, low_text) or (href and re.search(r, href, re.I)):
                                    skip = True
                                    break
                        if skip:
                            break

                    if skip:
                        continue

                    filtered.append((href, text, el))

                # parse filtered candidates
                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    title_candidate = anchor_text or ""
                    title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()

                    # Try extract location token from anchor text while preserving title text
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    if not title_clean:
                        title_clean = clean_title(title_candidate)
                    else:
                        # Ensure title gets cleaned for leftover boilerplate
                        title_clean = clean_title(title_clean)

                    # try card location extraction
                    card_loc = try_extract_location_from_card(el)
                    if card_loc and not location_candidate:
                        location_candidate = card_loc

                    posting_date = ""

                    # --- Detail decision logic ---
                    must_detail = False
                    must_reasons = []

                    if company.lower() in CRITICAL_COMPANIES:
                        must_detail = True
                        must_reasons.append("critical_company")

                    if not location_candidate:
                        must_detail = True
                        must_reasons.append("no_location")
                    if not posting_date:
                        must_detail = True
                        must_reasons.append("no_posting_date")
                    if len((title_clean or "").split()) < 2:
                        must_detail = True
                        must_reasons.append("short_title")

                    if LOC_RE.search(title_candidate):
                        must_detail = True
                        must_reasons.append("title_contains_loc_token")

                    if any(x in (link or "").lower() for x in [
                        "/job/", "/jobs/", "greenhouse", "lever.co", "ashbyhq",
                        "bamboohr", "myworkdayjobs", "gr8people",
                        "welcometothejungle", "career"
                    ]):
                        must_detail = True
                        must_reasons.append("link_looks_like_ats")

                    if must_detail:
                        print(f"[DETAIL_DECISION] company={company} link={link} reasons={','.join(must_reasons)} detail_count={detail_count}")

                    # Perform detail scraping
                    detail_html = ""
                    s = None
                    if must_detail and detail_count < MAX_DETAIL_PAGES:
                        detail_count += 1
                        print(f"[DETAIL_FETCH] #{detail_count} -> {company} -> {link}")

                        detail_html = fetch_page_content(page, link, nav_timeout=PAGE_NAV_TIMEOUT, dom_timeout=PAGE_DOM_TIMEOUT)

                        if detail_html:
                            try:
                                s = BeautifulSoup(detail_html, "lxml")

                                # H1/title fallback
                                header = s.find("h1")
                                if header:
                                    old_title = title_clean
                                    title_clean = clean_title(header.get_text(" ", strip=True))
                                    if title_clean != old_title:
                                        print(f"[DETAIL_TITLE] replaced '{old_title}' -> '{title_clean}'")

                                # location selectors
                                found_loc = None
                                for sel in [
                                    "span.location", ".job-location", ".location", "[data-test='job-location']",
                                    ".posting-location", ".job_meta_location", ".location--name",
                                    ".opening__meta", ".job-card__location", ".posting__location"
                                ]:
                                    eloc = s.select_one(sel)
                                    if eloc and eloc.get_text(strip=True):
                                        found_loc = normalize_location(eloc.get_text(" ", strip=True))
                                        break

                                if found_loc:
                                    location_candidate = found_loc
                                    print(f"[DETAIL_LOC] found -> {location_candidate}")

                                # JSON-LD robust parsing (jobLocation / datePosted)
                                for script in s.find_all("script", type="application/ld+json"):
                                    text = script.string
                                    if not text:
                                        continue

                                    payload = None
                                    for attempt in (text, "[" + text + "]"):
                                        try:
                                            payload = json.loads(attempt)
                                            break
                                        except:
                                            cleaned = re.sub(r'[\x00-\x1f]+', ' ', text)
                                            try:
                                                payload = json.loads(cleaned)
                                                break
                                            except:
                                                payload = None

                                    if not payload:
                                        continue

                                    items = payload if isinstance(payload, list) else [payload]

                                    for item in items:
                                        if not isinstance(item, dict):
                                            continue

                                        # jobLocation parsing
                                        jl = item.get("jobLocation") or item.get("jobLocations")
                                        if jl:
                                            jl_entry = jl[0] if isinstance(jl, list) else jl
                                            if isinstance(jl_entry, dict):
                                                addr = jl_entry.get("address") or jl_entry
                                                if isinstance(addr, dict):
                                                    parts = []
                                                    for k in ("addressLocality","addressRegion","addressCountry","postalCode"):
                                                        v = addr.get(k)
                                                        if v:
                                                            parts.append(str(v))
                                                    if parts:
                                                        location_candidate = normalize_location(", ".join(parts))
                                                        print(f"[DETAIL_JSON_LOC] found -> {location_candidate}")
                                                        break

                                                if jl_entry.get("name"):
                                                    location_candidate = normalize_location(str(jl_entry.get("name")))
                                                    print(f"[DETAIL_JSON_LOC] found -> {location_candidate}")
                                                    break

                                            elif isinstance(jl_entry, str):
                                                location_candidate = normalize_location(jl_entry)
                                                print(f"[DETAIL_JSON_LOC] found -> {location_candidate}")
                                                break

                                        # datePosted parsing
                                        if isinstance(item.get("datePosted"), str) and not posting_date:
                                            posting_date = _iso_only_date(item.get("datePosted"))
                                            print(f"[DETAIL_DATE] found -> {posting_date}")

                                # --- EXTENDED ATS-SPECIFIC DATE PARSERS ---
                                if not posting_date:
                                    try:
                                        text_blob = detail_html

                                        # Workday / Greenhouse / Lever / NextJS deep searches
                                        # (reuse extract_date_from_html for broad heuristics)
                                        found = extract_date_from_html(text_blob)
                                        if found:
                                            posting_date = found
                                            print(f"[DETAIL_AUX_DATE] extract_date_from_html -> {posting_date}")

                                        # additional script-key scans (BambooHR style)
                                        if not posting_date:
                                            for script in s.find_all("script"):
                                                t = script.string or script.text or ""
                                                if not t:
                                                    continue
                                                for key in ("posted_at","post_date","date_posted","datePosted","date_published","created_at","createdAt","postedOn"):
                                                    mkey = re.search(rf'"{key}"\s*:\s*"([^"]+)"', t, re.I)
                                                    if mkey:
                                                        posting_date = _iso_only_date(mkey.group(1))
                                                        print(f"[DETAIL_AUX_DATE] script-key {key} -> {posting_date}")
                                                        break
                                                if posting_date:
                                                    break

                                    except Exception as e:
                                        print(f"[WARN] extended-ats-date extractor failed: {e}")

                                # --- DATE NEAR TITLE PARSER ---
                                if not posting_date and s:
                                    try:
                                        h1 = s.find("h1")
                                        if h1:
                                            h1_block = h1.get_text(" ", strip=True)
                                            parent_block = h1.parent.get_text(" ", strip=True) if h1.parent else ""
                                            combined = h1_block + " " + parent_block
                                            m_iso = re.search(r'(\d{4}-\d{2}-\d{2})', combined)
                                            if m_iso:
                                                posting_date = _iso_only_date(m_iso.group(1))
                                                print(f"[TITLE_DATE] ISO near title -> {posting_date}")
                                            if not posting_date:
                                                m_days = re.search(r'posted\s+(\d+)\s+days?\s+ago', combined, re.I)
                                                if m_days:
                                                    d = date.today() - timedelta(days=int(m_days.group(1)))
                                                    posting_date = d.isoformat()
                                                    print(f"[TITLE_DATE] days-ago near title -> {posting_date}")
                                    except Exception as e:
                                        print(f"[WARN] title-date-extractor error: {e}")

                            except Exception as e:
                                print(f"[WARN] detail parse fail {link} -> {e}")

                    # final normalization and record
                    title_final = clean_title(title_clean) if title_clean else clean_title(anchor_text or "")
                    location_final = normalize_location(location_candidate)
                    posting_date_final = posting_date or ""

                    # --- ULTRA LOCATION EXTRACTOR (Patch 2 improvements) ---
                    if not location_candidate and detail_html:
                        try:
                            # Workday / addressLocality
                            m = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', detail_html)
                            if m:
                                city = m.group(1)
                                m2 = re.search(r'"addressCountry"\s*:\s*"([^"]+)"', detail_html)
                                country = m2.group(1) if m2 else ""
                                loc = f"{city}, {country}".strip(", ")
                                if loc:
                                    location_candidate = normalize_location(loc)
                                    print(f"[DEEP_LOC] Workday -> {location_candidate}")

                            # Greenhouse additionalLocations
                            if not location_candidate:
                                m = re.search(r'"additionalLocations"\s*:\s*\[(.+?)\]', detail_html, re.S)
                                if m:
                                    locs_raw = m.group(1)
                                    locs = re.findall(r'"([^"]+)"', locs_raw)
                                    if locs:
                                        location_candidate = normalize_location(locs[0])
                                        print(f"[DEEP_LOC] Greenhouse additionalLocations -> {location_candidate}")

                            # Lever categories.location
                            if not location_candidate:
                                m = re.search(r'"categories"\s*:\s*{[^}]*"location"\s*:\s*"([^"]+)"', detail_html)
                                if m:
                                    location_candidate = normalize_location(m.group(1))
                                    print(f"[DEEP_LOC] Lever -> {location_candidate}")

                            # Ashby locations array
                            if not location_candidate:
                                m = re.search(r'"locations"\s*:\s*\[(.+?)\]', detail_html, re.S)
                                if m:
                                    js = m.group(1)
                                    city = re.search(r'"city"\s*:\s*"([^"]+)"', js)
                                    region = re.search(r'"region"\s*:\s*"([^"]+)"', js)
                                    country = re.search(r'"country"\s*:\s*"([^"]+)"', js)
                                    parts = [x.group(1) for x in (city, region, country) if x]
                                    if parts:
                                        location_candidate = normalize_location(", ".join(parts))
                                        print(f"[DEEP_LOC] Ashby -> {location_candidate}")

                            # breadcrumbs
                            if not location_candidate and s:
                                crumbs = s.select("nav a, .breadcrumb a, .breadcrumbs a")
                                for cr in crumbs:
                                    txt = cr.get_text(" ", strip=True)
                                    if LOC_RE.search(txt):
                                        location_candidate = normalize_location(txt)
                                        print(f"[DEEP_LOC] breadcrumb -> {location_candidate}")
                                        break

                        except Exception as e:
                            print(f"[WARN] deep-loc extractor error: {e}")

                    # --- ULTRA_LOC regex fallback (Patch 2) ---
                    if not location_candidate and detail_html:
                        snippet = detail_html[:60000]
                        mm = re.search(
                            r'([A-Z][a-zA-Z]+(?:[ \-][A-Z][a-zA-Z]+)*)[,\s\-–]+(USA|United States|UK|Germany|France|India|Singapore|Canada|Australia|Netherlands|Switzerland|Ireland)',
                            snippet
                        )
                        if mm:
                            location_candidate = normalize_location(mm.group(0))
                            print(f"[ULTRA_LOC_PATCH2] -> {location_candidate}")

                    location_final = normalize_location(location_candidate)

                    # fallback: try extract location from link path
                    if not location_final:
                        mloc = re.search(r'/(remote|new[-_]york|london|berlin|singapore|bengaluru|chennai|munich|frankfurt)[/\-]?', link or "", re.I)
                        if mloc:
                            location_final = mloc.group(1).replace('-', ' ').title()

                    # posting date from anchor text fallback
                    if not posting_date_final:
                        posted_from_anchor = re.search(r'posted\s+(\d+)\s+days?\s+ago', anchor_text or "", re.I)
                        if posted_from_anchor:
                            d = date.today() - timedelta(days=int(posted_from_anchor.group(1)))
                            posting_date_final = d.isoformat()

                    rows.append({
                        "Company": company,
                        "Job Title": title_final,
                        "Job Link": link,
                        "Location": location_final,
                        "Posting Date": posting_date_final,
                        "Days Since Posted": ""
                    })

        browser.close()
        return rows # Return the rows list

# --- MAIN EXECUTION LOGIC (Moved outside scrape function) ---
if __name__ == "__main__":
    try:
        all_rows = scrape()

        # inject seniority into rows BEFORE dedupe sorting
        for r in all_rows:
            r["Seniority"] = detect_seniority(r.get("Job Title",""))

        # dedupe by Job Link and compute Days Since Posted
        dedup = {}
        for r in all_rows:
            lk = r.get("Job Link") or ""
            if lk in dedup:
                continue
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
        out_sorted = sorted(out, key=lambda x: (x.get("Company","").lower(), x.get("Job Title","").lower()))

        outfile = "jobs_final_hard.csv"
        # The original code had the 'Days Since Posted' column *in* the writer fieldnames list, but *not* the 'Seniority' column.
        # To include 'Seniority', add it to the fieldnames list:
        # fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Seniority"]
        fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted"]
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in out_sorted:
                # Filter out the 'Seniority' key if it's not in the fieldnames to prevent an error
                row_to_write = {k: v for k, v in r.items() if k in fieldnames}
                writer.writerow(row_to_write)

        print(f"[OK] wrote {len(out_sorted)} rows -> {outfile}")

    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during execution: {e}")
        sys.exit(1)
