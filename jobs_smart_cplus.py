# jobs_smart_cplus_fixed.py (REPAIRED)
# Playwright + BeautifulSoup hybrid scraper, ATS-aware, safe detail quota.
# Run with: python3 jobs_smart_cplus_fixed.py

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
PAGE_DOM_TIMEOUT = 15000       # shorter detail DOM timeout
SLEEP_BETWEEN_REQUESTS = 0.18
MAX_DETAIL_PAGES = 9999        # increased quota to capture needed details

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

ROLE_WORDS_RE = re.compile(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|specialist)\b', re.I)

# Company-specific skip rules to avoid non-job pages (tweak as needed)
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
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = FORBIDDEN_RE.sub('', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.")
    return t


def normalize_location(loc):
    if not loc:
        return ""
    s = re.sub(r'[\n\r\t]', ' ', loc)
    parts = [p.strip() for p in re.split(r'[,/;|]+', s) if p.strip()]
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        if key == "remote":
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
    if not html_text:
        return ""
    # JSON-LD datePosted
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        return _iso_only_date(m.group(1))
    # Workday-like postedOn
    m2 = re.search(r'"postedOn"\s*:\s*"([^"]+)"', html_text)
    if m2:
        return _iso_only_date(m2.group(1))
    # meta property article:published_time
    m3 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
    if m3:
        return _iso_only_date(m3.group(1))
    # <time datetime="">
    m4 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m4:
        return _iso_only_date(m4.group(1))
    # textual "posted X days ago"
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        days = int(mm.group(1)); return (date.today() - timedelta(days=days)).isoformat()
    # fallback: YYYY-MM-DD anywhere
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
    positives = ['jobs', '/job/', '/jobs/', 'careers', 'open-positions', 'openings',
                 'greenhouse', 'lever.co', 'myworkdayjobs', 'bamboohr', 'ashby', 'comeet',
                 'gr8people', 'boards.greenhouse', '/job/']
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
    # parentheses at end -> often location
    paren = re.search(r'\(([^)]+)\)\s*$', s)
    if paren:
        loc = paren.group(1); title = s[:paren.start()].strip(" -:,"); return title, normalize_location(loc)
    parts = re.split(r'\s{2,}| - | — | – | \| |·|•| — |,', s)
    parts = [p.strip() for p in parts if p and p.strip()]
    if len(parts) >= 2:
        last = parts[-1]
        if LOC_RE.search(last) or (len(last.split()) <= 4 and re.search(r'[A-Za-z]', last)):
            return " ".join(parts[:-1]), normalize_location(last)
    m = LOC_RE.search(s)
    if m:
        idx = m.start(); candidate = s[idx:].strip(" -,:;"); candidate = re.split(r'\s{2,}| - | — | – | \| ', candidate)[0].strip()
        title = s.replace(candidate, '').strip(" -:,"); return title, normalize_location(candidate)
    return s, ""


def try_extract_location_from_card(el):
    if not el:
        return ""
    search_selectors = [
        ".location", ".job-location", "span[class*='location']", ".posting-location",
        ".job_meta_location", ".location--name", ".job-card__location", ".opening__meta",
        ".job-meta__location", ".opening__location", ".jobCard-location", ".locationTag"
    ]

    def attr_location(tag):
        if not tag:
            return ""
        for attr in ("data-location", "data-geo", "aria-label", "title", "data-qa-location", "data-test-location"):
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
            cand = re.split(r'\s{2,}| - | \| |—|–|,', nearby_text)[-1].strip()
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
                    href = a.get("href"); text = (a.get_text(" ", strip=True) or "")
                    href_abs = normalize_link(main_url, href)
                    if is_likely_job_anchor(href_abs, text):
                        candidates.append((href_abs, text, a))

                # job-card containers
                for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"):
                    a = el.find("a", href=True)
                    text = (a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True))
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text, el))

                # try iframe to ATS if none found
                if not candidates:
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse","lever","myworkday","bamboohr","ashby","jobs.lever")):
                            src_full = normalize_link(main_url, src)
                            iframe_html = fetch_page_content(page, src_full)
                            if iframe_html:
                                f_soup = BeautifulSoup(iframe_html, "lxml")
                                for a in f_soup.find_all("a", href=True):
                                    href = a.get("href"); text = (a.get_text(" ", strip=True) or "")
                                    href_abs = normalize_link(src_full, href)
                                    if is_likely_job_anchor(href_abs, text):
                                        candidates.append((href_abs, text, a))

                # dedupe preserve order and apply company specific skip rules
                seen = set(); filtered = []
                for href, text, el in candidates:
                    if not href or href.rstrip("/") == main_url.rstrip("/"):
                        continue
                    if href in seen: continue
                    seen.add(href)
                    skip = False
                    low_text = (text or "").lower()
                    for c, rules in COMPANY_SKIP_RULES.items():
                        if c.lower() == company.lower():
                            for r in rules:
                                if re.search(r, low_text) or (href and re.search(r, href, re.I)):
                                    skip = True; break
                        if skip: break
                    if skip: continue
                    filtered.append((href, text, el))

                # parse filtered candidates
                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    title_candidate = anchor_text or ""
                    title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    if not title_clean:
                        title_clean = clean_title(title_candidate)

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
                                            
                                # --- UNIVERSAL DEEP DATE EXTRACTOR ---
                                if not posting_date:
                                    try:
                                        # 1) Search ANY ISO date inside any script tag
                                        for script in s.find_all("script"):
                                            t = script.string or script.text or ""
                                            if not t:
                                                continue

                                            # ISO formats: 2024-11-20 / 2024-11-20T10:30:00Z
                                            m = re.search(r'(\d{4}-\d{2}-\d{2})(?:[T ][\d:Z+-]*)?', t)
                                            if m:
                                                posting_date = _iso_only_date(m.group(1))
                                                print(f"[DETAIL_DEEP_DATE] ISO found -> {posting_date}")
                                                break

                                            # 2) Workday-style
                                            m2 = re.search(r'"posted(Date|On)"\s*:\s*"([^"]+)"', t, re.I)
                                            if m2:
                                                posting_date = _iso_only_date(m2.group(2))
                                                print(f"[DETAIL_DEEP_DATE] Workday -> {posting_date}")
                                                break

                                            # 3) Lever createdAt
                                            m3 = re.search(r'"createdAt"\s*:\s*"([^"]+)"', t)
                                            if m3:
                                                posting_date = _iso_only_date(m3.group(1))
                                                print(f"[DETAIL_DEEP_DATE] Lever createdAt -> {posting_date}")
                                                break

                                            # 4) Greenhouse updated_at
                                            m4 = re.search(r'"updated_at"\s*:\s*"([^"]+)"', t)
                                            if m4:
                                                posting_date = _iso_only_date(m4.group(1))
                                                print(f"[DETAIL_DEEP_DATE] Greenhouse updated_at -> {posting_date}")
                                                break

                                            # 5) ATS generic posted keys
                                            m5 = re.search(r'"(posted|postingDate|date_created)"\s*:\s*"([^"]+)"', t, re.I)
                                            if m5:
                                                posting_date = _iso_only_date(m5.group(2))
                                                print(f"[DETAIL_DEEP_DATE] ATS generic -> {posting_date}")
                                                break

                                        # FINAL HTML SCAN
                                        if not posting_date:
                                            m6 = re.findall(r'\d{4}-\d{2}-\d{2}', detail_html)
                                            if m6:
                                                posting_date = m6[0]
                                                print(f"[DETAIL_DEEP_DATE] HTML fallback -> {posting_date}")

                                    except Exception as e:
                                        print(f"[WARN] deep date extractor failed: {e}")
                                        
                                # fallback: extract date via regex from entire HTML if still missing
                                if not posting_date:
                                    found_date = extract_date_from_html(detail_html)
                                    if found_date:
                                        posting_date = found_date
                                        print(f"[DETAIL_DATE_FALLBACK] parsed -> {posting_date}")

                            except Exception as e:
                                print(f"[WARN] detail parse fail {link} -> {e}")

                    # final normalization and record
                    title_final = clean_title(title_clean) if title_clean else clean_title(anchor_text or "")
                    location_final = normalize_location(location_candidate)
                    posting_date_final = posting_date or ""

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

    # dedupe by Job Link and compute Days Since Posted
    dedup = {}
    for r in rows:
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
