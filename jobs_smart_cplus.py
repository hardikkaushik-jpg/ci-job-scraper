# jobs_smart_cplus.py  — Refactored v5.0
# Fixes applied:
#   1. Double-scraping bug: generic pipeline is SKIPPED when a special extractor runs
#   2. URL normalisation before deduplication
#   3. Per-company row cap (300) with hard warning
#   4. first_seen / last_seen lifecycle columns
#   5. Consistent 5-tuple output contract from all code paths
#   6. Detail fetching writes description into row for enrichment downstream

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import re, csv, time, sys, json, os
from datetime import datetime, date, timedelta

try:
    from special_extractors_deep import SPECIAL_EXTRACTORS_DEEP
except ImportError:
    SPECIAL_EXTRACTORS_DEEP = {}

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
COMPANIES = {
    "Airtable":           ["https://airtable.com/careers#open-positions"],
    "Alation":            ["https://www.alation.com/careers/all-careers/"],
    "Alteryx":            ["https://alteryx.wd108.myworkdayjobs.com/AlteryxCareers"],
    "Ataccama":           ["https://jobs.ataccama.com/#one-team"],
    "Atlan":              ["https://atlan.com/careers"],
    "Anomalo":            ["https://boards.greenhouse.io/anomalojobs"],
    "BigEye":             ["https://www.bigeye.com/careers#positions"],
    "Boomi":              ["https://boomi.com/company/careers/#greenhouseapp"],
    "CastorDoc":          ["https://jobs.ashbyhq.com/coalesce"],
    "Cloudera":           ["https://cloudera.wd5.myworkdayjobs.com/External_Career"],
    "Collibra":           ["https://www.collibra.com/company/careers#sub-menu-find-jobs"],
    "Couchbase":          ["https://www.couchbase.com/careers/"],
    "Data.World":         ["https://data.world/company/careers/#careers-list"],
    "Databricks":         ["https://www.databricks.com/company/careers/open-positions"],
    "Datadog":            ["https://careers.datadoghq.com/all-jobs/"],
    "DataGalaxy":         ["https://www.welcometothejungle.com/en/companies/datagalaxy/jobs"],
    "Decube":             ["https://boards.briohr.com/bousteaduacmalaysia-4hu7jdne41"],
    "Exasol":             ["https://careers.exasol.com/en/jobs"],
    "Firebolt":           ["https://www.firebolt.io/careers"],
    "Fivetran":           ["https://www.fivetran.com/careers#jobs"],
    "InfluxData":         ["https://www.influxdata.com/careers/#jobs"],
    "Informatica":        ["https://informatica.gr8people.com/jobs"],
    "Matillion":          ["https://jobs.lever.co/matillion"],
    "MongoDB":            ["https://www.mongodb.com/company/careers/teams/engineering",
                           "https://www.mongodb.com/company/careers/teams/product-management-and-design"],
    "Monte Carlo":        ["https://jobs.ashbyhq.com/montecarlodata"],
    "Oracle":             ["https://careers.oracle.com/en/sites/jobsearch/jobs"],
    "Precisely":          ["https://www.precisely.com/careers-and-culture/us-jobs"],
    "Pentaho":            ["https://www.hitachivantara.com/en-us/company/careers/job-search"],
    "Qlik":               ["http://careerhub.qlik.com/careers"],
    "Sifflet":            ["https://www.welcometothejungle.com/en/companies/sifflet/jobs"],
    "Snowflake":          ["https://careers.snowflake.com/global/en/search-results"],
    "Syniti":             ["https://careers.syniti.com/go/Explore-Our-Roles/8777900/"],
    "Teradata":           ["https://careers.teradata.com/jobs"],
    "Vertica":            ["https://careers.opentext.com/us/en/home"],
    "Salesforce":         ["https://careers.salesforce.com/en/jobs/"],
    "Amazon":             ["https://www.amazon.jobs/en/"],
    "IBM":                ["https://www.ibm.com/careers/search"],
    "SAP":                ["https://jobs.sap.com/"],
    # ── Vector DB / AI-native storage ──────────────────────────────────────────
    "Pinecone":           ["https://jobs.ashbyhq.com/pinecone"],
    "Weaviate":           ["https://www.welcometothejungle.com/en/companies/weaviate/jobs"],
    "Qdrant":             ["https://qdrant.tech/careers/"],
    "Zilliz":             ["https://jobs.lever.co/zilliz"],
}

PAGE_NAV_TIMEOUT     = 40_000
PAGE_DOM_TIMEOUT     = 15_000
SLEEP_BETWEEN        = 0.18
MAX_DETAIL_PAGES     = 12_000
PER_COMPANY_ROW_CAP  = 300          # hard warning threshold
PER_COMPANY_CAP_OVERRIDES = {
    "Databricks": 500,   # large company, legitimately >300 after filtering
    "MongoDB":    450,
    "Fivetran":   200,
}

TODAY = date.today().isoformat()

# Companies with JS-heavy portals — given longer timeout
SLOW_COMPANIES = {"SAP", "IBM", "Salesforce", "Amazon", "Oracle"}

# These use APIs returning complete 5-tuples — skip detail page fetch entirely
API_COMPLETE_COMPANIES = {
    "Databricks", "Collibra", "Fivetran", "MongoDB", "Boomi",
    "Matillion", "Anomalo", "Atlan", "Pinecone", "Zilliz",
    "Monte Carlo", "Datadog",
}

# ─────────────────────────────────────────────────────────────────────────────
# PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
FORBIDDEN_RE = re.compile(
    r'\b(?:privacy|about|press|blog|partners|pricing|docs|support|events|'
    r'resources|login|apply now|read more)\b', re.I
)
LOC_RE = re.compile(
    r'\b(?:remote|hybrid|usa|united states|uk|germany|india|london|new york|'
    r'singapore|berlin|bengaluru)\b', re.I
)
ROLE_WORDS_RE = re.compile(
    r'\b(?:engineer|developer|manager|director|architect|scientist|analyst|'
    r'product|sre|intern|specialist|consultant|lead|staff|principal)\b', re.I
)
COMPANY_SKIP_RULES = {
    "Ataccama": [r'one-team', r'blog', r'about'],
    "Fivetran":  [r'launchers', r'developer-relations'],
    "Datadog":   [r'resources', r'events', r'learning'],
    "BigEye":    [r'product', r'resources'],
}
CRITICAL_COMPANIES = {
    "fivetran", "ataccama", "datadog", "snowflake",
    "matillion", "oracle", "mongodb", "databricks"
}
RELEVANCE_HARD = [
    r'\bdata engineer\b', r'\betl\b', r'\bintegrat',
    r'\bconnector', r'\bpipeline\b', r'\bsnowflake\b',
    r'\bdatabricks\b', r'\bobservab',
    r'\bgovernance\b', r'\bcatalog\b', r'\blineage\b',
    r'\bmetadata\b', r'\bdata quality\b', r'\bdata mesh\b',
    r'\bvector\b', r'\bembedding\b', r'\brag\b', r'\bllm\b',
]
RELEVANCY_THRESHOLD = 2

NON_TECH_PRODUCT_PATTERNS = [
    r'product\s+marketing', r'product\s+marketer', r'product\s+g?tm',
    r'product\s+operations', r'product\s+ops', r'product\s+design(er)?',
    r'product\s+growth', r'product\s+strategy', r'product\s+enablement',
    r'product\s+commercial', r'marketing\s+product',
]
PRODUCT_TECH_KEYWORDS = [
    "etl", "pipeline", "connector", "integration", "api", "sdk",
    "snowflake", "databricks", "warehouse", "lakehouse", "airflow",
    "spark", "bigquery", "postgres", "mysql", "orchestration",
    "kafka", "kubernetes", "observability",
]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def normalise_url(url: str) -> str:
    """Strip query params, fragments, and trailing slashes for deduplication."""
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        # keep scheme + netloc + path only, lowercase netloc
        clean = urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", "", ""))
        return clean
    except Exception:
        return url.strip().rstrip("/")


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
    except Exception:
        return href


def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = FORBIDDEN_RE.sub('', t)
    t = re.sub(r'\((?:remote|hybrid)[^)]+\)\s*$', '', t, flags=re.I).strip()
    t = re.sub(r'^[\-•\*]\s*', '', t)
    return t.strip(" -:,.|")[:240]


def extract_location_from_text(txt):
    if not txt:
        return "", ""
    s = txt.replace("\r", " ").replace("\n", " ").strip()
    paren = re.search(r'\(([^)]+)\)\s*$', s)
    if paren:
        return s[:paren.start()].strip(" -:,"), paren.group(1)
    parts = re.split(r'\s{2,}| - | — | – | \| |·|•|,', s)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2 and (LOC_RE.search(parts[-1]) or len(parts[-1].split()) <= 4):
        return " ".join(parts[:-1]), parts[-1]
    m = LOC_RE.search(s)
    if m:
        idx = m.start()
        candidate = s[idx:].strip(" -,:;")
        title = s.replace(candidate, "").strip(" -:,")
        return title, candidate
    return s, ""


def try_extract_location_from_card(el):
    if not el:
        return ""
    for sel in [".location", ".job-location", ".posting-location", ".job_meta_location"]:
        try:
            found = el.select_one(sel)
        except Exception:
            found = None
        if found and found.get_text(strip=True):
            return found.get_text(" ", strip=True)
    for attr in ("data-location", "data-geo", "aria-label", "title"):
        v = el.get(attr)
        if v and isinstance(v, str):
            return v
    return ""


def detect_seniority(title):
    if not title:
        return "Unknown"
    t = title.lower()
    if any(x in t for x in ["chief ", "cto", "vp ", "director", "head of"]):
        return "Director+"
    if any(x in t for x in ["principal", "distinguished", "staff "]):
        return "Principal/Staff"
    if any(x in t for x in ["senior", "sr.", "lead "]):
        return "Senior"
    if any(x in t for x in ["manager", "mgr"]):
        return "Manager"
    if any(x in t for x in ["mid ", "associate", " ii"]):
        return "Mid"
    if any(x in t for x in ["junior", "jr.", "entry"]):
        return "Entry"
    if any(x in t for x in ["intern", "internship", "werkstudent"]):
        return "Intern"
    return "Unknown"


def is_likely_job_anchor(href, text):
    if not href:
        return False
    h = (href or "").lower()
    t = (text or "").lower()
    BAD = ["about", "privacy", "press", "events", "product",
           "resources", "download", "company", "blog"]
    if any(b in h for b in BAD) or any(b in t for b in BAD):
        return False
    ATS = ["lever.co", "greenhouse", "myworkdayjobs", "ashby",
           "bamboohr", "smartrecruiters", "jobvite", "/jobs/", "/job/"]
    if any(a in h for a in ATS):
        return True
    if ROLE_WORDS_RE.search(t):
        return True
    return False


def should_drop_by_title(title):
    if not title or not title.strip():
        return True
    if ROLE_WORDS_RE.search((title or "").lower()):
        return False
    if re.search(r'\bintern\b', (title or "").lower()):
        return False
    return True


def _iso_only_date(raw):
    if not raw:
        return ""
    raw = raw.strip()
    try:
        return datetime.fromisoformat(raw.replace("Z", "")).date().isoformat()
    except Exception:
        pass
    try:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""


def extract_date_from_html(html_text):
    if not html_text:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        try:
            return datetime.fromisoformat(m.group(1).split("T")[0]).date().isoformat()
        except Exception:
            pass
    m2 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m2:
        try:
            return datetime.fromisoformat(m2.group(1).split("T")[0]).date().isoformat()
        except Exception:
            pass
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        try:
            return (date.today() - timedelta(days=int(mm.group(1)))).isoformat()
        except Exception:
            pass
    mm2 = re.search(r'(\d{4}-\d{2}-\d{2})', html_text)
    if mm2:
        return mm2.group(1)
    return ""


def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT):
    try:
        page.goto(url, timeout=nav_timeout, wait_until="networkidle")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(900)
        return page.content()
    except Exception:
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            page.wait_for_timeout(900)
            return page.content()
        except Exception:
            return ""


def score_title_desc(title, desc, company=""):
    t = ((title or "") + " " + (desc or "")).lower()
    score = 0
    for p in RELEVANCE_HARD:
        if re.search(p, t):
            score += 3
    if company and "oracle" in company.lower() and "autonomous" in t:
        score += 1
    return score


def enrich_detail(page, link, detail_count, location_in, date_in):
    """
    Fetch a detail page and extract location, posting date, and description text.
    Returns (location, posting_date, description, new_detail_count).
    """
    if detail_count >= MAX_DETAIL_PAGES:
        return location_in, date_in, "", detail_count

    detail_count += 1
    detail_html = fetch_page_content(page, link)
    location_out = location_in
    date_out = date_in
    desc_out = ""

    if not detail_html:
        return location_out, date_out, desc_out, detail_count

    try:
        s = BeautifulSoup(detail_html, "lxml")

        # Location selectors
        for sel in ["span.location", ".job-location", ".location",
                    "[data-test='job-location']", ".posting-location",
                    ".job_meta_location", ".location--name"]:
            eloc = s.select_one(sel)
            if eloc and eloc.get_text(strip=True):
                location_out = eloc.get_text(" ", strip=True)
                break

        # Description text (first 4000 chars)
        for sel in [".job-description", "#job-description", ".description",
                    "[data-automation-id='jobPostingDescription']",
                    ".content", "article", "main"]:
            el = s.select_one(sel)
            if el:
                desc_out = el.get_text(" ", strip=True)[:4000]
                break

        # JSON-LD for date and location
        for script in s.find_all("script", type="application/ld+json"):
            raw = script.string or ""
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            items = payload if isinstance(payload, list) else [payload]
            for obj in items:
                if not isinstance(obj, dict):
                    continue
                if not date_out and isinstance(obj.get("datePosted"), str):
                    date_out = _iso_only_date(obj["datePosted"])
                if not location_out:
                    jl = obj.get("jobLocation") or obj.get("jobLocations")
                    if jl:
                        entry = jl[0] if isinstance(jl, list) else jl
                        if isinstance(entry, dict):
                            addr = entry.get("address") or entry
                            if isinstance(addr, dict):
                                parts = [str(addr.get(k)) for k in
                                         ("addressLocality", "addressRegion",
                                          "addressCountry") if addr.get(k)]
                                if parts:
                                    location_out = ", ".join(parts)

        if not date_out:
            date_out = extract_date_from_html(detail_html)

    except Exception as e:
        print(f"[WARN] detail parse fail {link} -> {e}")

    return location_out, date_out, desc_out, detail_count


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPE
# ─────────────────────────────────────────────────────────────────────────────
def scrape():
    rows = []
    detail_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        for company, url_list in COMPANIES.items():
            company_rows = []
            import time as _time
            company_start = _time.time()
            COMPANY_TIMEOUT_SECS = 900 if company in SLOW_COMPANIES else 600

            for main_url in url_list:
                # Per-company timeout check
                if _time.time() - company_start > COMPANY_TIMEOUT_SECS:
                    print(f"[TIMEOUT] {company} exceeded {COMPANY_TIMEOUT_SECS}s — skipping remaining URLs")
                    break

                print(f"\n[SCRAPING] {company} -> {main_url}")
                listing_html = fetch_page_content(page, main_url)
                if not listing_html:
                    print(f"[WARN] no html for {company} ({main_url})")
                    continue

                soup = BeautifulSoup(listing_html, "lxml")

                # ══════════════════════════════════════════════════════════
                # PATH A — SPECIAL EXTRACTOR
                # ══════════════════════════════════════════════════════════
                if company in SPECIAL_EXTRACTORS_DEEP:
                    print(f"[SPECIAL] Running extractor for {company}")
                    try:
                        raw_items = SPECIAL_EXTRACTORS_DEEP[company](soup, page, main_url)
                        print(f"[SPECIAL] {company}: {len(raw_items)} raw items")
                    except Exception as e:
                        print(f"[SPECIAL ERROR] {company} -> {e}")
                        # ── CRITICAL: skip generic pipeline even on extractor error ──
                        continue  # move to next URL for this company

                    for item in raw_items:
                        if not item:
                            continue

                        # Normalise to (link, title, desc, loc, date)
                        if len(item) == 5:
                            link, title, desc_text, loc_text, post_date = item
                        elif len(item) == 3:
                            link, title, _ = item
                            desc_text = loc_text = post_date = ""
                        elif len(item) == 2:
                            link, title = item
                            desc_text = loc_text = post_date = ""
                        else:
                            link = item[0]
                            title = item[1] if len(item) > 1 else ""
                            desc_text = loc_text = post_date = ""

                        t_candidate, loc_candidate = extract_location_from_text(title)
                        title_final = clean_title(t_candidate or title)

                        if should_drop_by_title(title_final):
                            print(f"[DROP-SPECIAL] {company} | {title_final}")
                            continue

                        location_final = (loc_text or loc_candidate or "").strip()

                        # Skip detail fetch for API companies — they already have full data
                        if company not in API_COMPLETE_COMPANIES and link and (
                            not location_final or not post_date or not desc_text
                        ):
                            location_final, post_date, desc_enriched, detail_count = enrich_detail(
                                page, link, detail_count, location_final, post_date
                            )
                            if not desc_text:
                                desc_text = desc_enriched

                        company_rows.append({
                            "Company": company,
                            "Job Title": title_final,
                            "Job Link": link,
                            "Location": location_final,
                            "Posting Date": post_date or "",
                            "Days Since Posted": "",
                            "Description": desc_text[:4000] if desc_text else "",
                        })
                        print(f"[KEEP-SPECIAL] {company} | {title_final}")

                    # ── SKIP GENERIC PIPELINE — this is the double-scraping fix ──
                    continue

                # ══════════════════════════════════════════════════════════
                # PATH B — GENERIC PIPELINE (only runs if no special extractor)
                # ══════════════════════════════════════════════════════════
                candidates = []

                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    text = a.get_text(" ", strip=True) or ""
                    href_abs = normalize_link(main_url, href)
                    if is_likely_job_anchor(href_abs, text):
                        candidates.append((href_abs, text, a))

                for el in soup.select("[data-job], .job, .job-listing, .job-card, "
                                      ".opening, .position, .posting, .role, .job-row"):
                    a = el.find("a", href=True)
                    text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text, el))

                if not candidates:
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in (
                                "greenhouse", "lever", "myworkday",
                                "bamboohr", "ashby", "jobvite")):
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

                # dedupe + skip rules
                seen_generic = set()
                filtered = []
                for href, text, el in candidates:
                    if not href or href.rstrip("/") == main_url.rstrip("/"):
                        continue
                    norm = normalise_url(href)
                    if norm in seen_generic:
                        continue
                    seen_generic.add(norm)
                    skip = False
                    low_text = (text or "").lower()
                    for c, rules in COMPANY_SKIP_RULES.items():
                        if c.lower() == company.lower():
                            for r in rules:
                                if re.search(r, low_text) or re.search(r, href, re.I):
                                    skip = True
                                    break
                        if skip:
                            break
                    if not skip:
                        filtered.append((href, text, el))

                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN)

                    jt_div = None
                    try:
                        jt_div = el.select_one("[data-automation-id='jobTitle']")
                    except Exception:
                        pass

                    title_candidate = (jt_div.get_text(" ", strip=True)
                                       if jt_div else
                                       re.sub(r'\s+', ' ', anchor_text or "").strip())

                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    title_clean = clean_title(title_clean or title_candidate)
                    title_low = title_clean.lower()

                    # Product filter
                    if "product" in title_low:
                        if any(re.search(p, title_low) for p in NON_TECH_PRODUCT_PATTERNS):
                            print(f"[DROP-PRODUCT-TITLE] {title_clean}")
                            continue

                    card_loc = try_extract_location_from_card(el)
                    if card_loc and not location_candidate:
                        location_candidate = card_loc

                    must_detail = (
                        company.lower() in CRITICAL_COMPANIES
                        or not location_candidate
                        or len((title_clean or "").split()) < 2
                        or LOC_RE.search(title_candidate)
                        or any(x in (link or "").lower() for x in [
                            "/job/", "/jobs/", "greenhouse", "lever.co",
                            "ashby", "bamboohr", "myworkdayjobs",
                            "gr8people", "welcometothejungle",
                        ])
                        or ("product" in title_low)
                    )

                    light_score = score_title_desc(title_candidate, "", company)
                    desc_text = ""

                    posting_date = ""
                    if light_score >= RELEVANCY_THRESHOLD and not must_detail:
                        print(f"[KEEP-LIGHT] {company} | {title_candidate} | score={light_score}")
                    else:
                        if light_score <= 0 and not must_detail:
                            print(f"[DROP-LIGHT] {company} | {title_candidate} score={light_score}")
                            continue

                        location_candidate, posting_date, desc_text, detail_count = enrich_detail(
                            page, link, detail_count, location_candidate, ""
                        )

                        # Detail-level product filter
                        if "product" in title_low:
                            if not any(k in (desc_text or "").lower() for k in PRODUCT_TECH_KEYWORDS):
                                print(f"[DROP-PRODUCT] {company} | {title_clean}")
                                continue

                        # H1 title override for generic pages
                        try:
                            detail_html_for_title = fetch_page_content(page, link) or ""
                            if detail_html_for_title:
                                s_detail = BeautifulSoup(detail_html_for_title, "lxml")
                                h1 = s_detail.find("h1")
                                if h1:
                                    newt = clean_title(h1.get_text(" ", strip=True))
                                    if newt and newt != title_clean:
                                        title_clean = newt
                        except Exception:
                            pass

                        final_score = score_title_desc(title_clean or title_candidate,
                                                       desc_text, company)
                        print(f"[FINAL_SCORE] {company} final={final_score}")
                        if final_score < RELEVANCY_THRESHOLD:
                            print(f"[DROP-FINAL] {company} | {title_clean}")
                            continue

                    final_title, loc_from_title = extract_location_from_text(title_clean)
                    final_title = clean_title(final_title)
                    if loc_from_title and not location_candidate:
                        location_candidate = loc_from_title
                    final_location = (location_candidate or loc_from_title or "").strip()

                    # Final product filter after cleanup
                    if "product" in final_title.lower():
                        if not any(k in (desc_text or "").lower() for k in PRODUCT_TECH_KEYWORDS):
                            print(f"[DROP-PRODUCT-FINAL] {final_title}")
                            continue

                    if not should_drop_by_title(final_title):
                        company_rows.append({
                            "Company": company,
                            "Job Title": final_title,
                            "Job Link": link,
                            "Location": final_location,
                            "Posting Date": posting_date if 'posting_date' in dir() else "",
                            "Days Since Posted": "",
                            "Description": desc_text[:4000] if desc_text else "",
                        })
                    else:
                        print(f"[DROP] {company} | {final_title}")

            # Per-company row cap check
            cap = PER_COMPANY_CAP_OVERRIDES.get(company, PER_COMPANY_ROW_CAP)
            if len(company_rows) > cap:
                print(f"[ANOMALY WARNING] {company} produced {len(company_rows)} rows "
                      f"(cap={cap}). Investigate before trusting this data.")

            rows.extend(company_rows)

        browser.close()
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        all_rows = scrape()

        # Seniority
        for r in all_rows:
            r["Seniority"] = detect_seniority(r.get("Job Title", ""))

        # Load existing CSV for first_seen tracking
        repo_root = os.path.dirname(os.path.abspath(__file__))
        outfile   = os.path.join(repo_root, "jobs_final_hard.csv")

        existing_first_seen = {}
        if os.path.exists(outfile):
            try:
                with open(outfile, encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        lk = normalise_url(row.get("Job Link", ""))
                        if lk and row.get("First_Seen"):
                            existing_first_seen[lk] = row["First_Seen"]
            except Exception:
                pass

        # Deduplication using normalised URLs
        dedup = {}
        for r in all_rows:
            norm_lk = normalise_url(r.get("Job Link", ""))
            if not norm_lk:
                continue
            if norm_lk in dedup:
                # prefer row with posting date
                if not dedup[norm_lk].get("Posting Date") and r.get("Posting Date"):
                    dedup[norm_lk] = r
                continue

            # Days since posted
            pd = r.get("Posting Date") or ""
            if pd:
                try:
                    r["Days Since Posted"] = str(
                        (date.today() - datetime.fromisoformat(pd).date()).days)
                except Exception:
                    r["Days Since Posted"] = ""

            # Lifecycle columns
            r["First_Seen"] = existing_first_seen.get(norm_lk, TODAY)
            r["Last_Seen"]  = TODAY

            dedup[norm_lk] = r

        out = sorted(dedup.values(),
                     key=lambda x: (x.get("Company","").lower(),
                                    x.get("Job Title","").lower()))

        fieldnames = [
            "Company", "Job Title", "Job Link", "Location",
            "Posting Date", "Days Since Posted", "Seniority",
            "Description", "First_Seen", "Last_Seen",
        ]

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for r in out:
                writer.writerow({k: r.get(k, "") or "" for k in fieldnames})

        print(f"\n[OK] wrote {len(out)} deduplicated rows -> {outfile}")

    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] {e}")
        raise
