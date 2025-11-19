# jobs_smart_cplus_final_full_mod.py
# Playwright + BeautifulSoup hybrid scraper, ATS-aware, enhanced cleaning and classification.
# Run with: python3 jobs_smart_cplus_final_full_mod.py
# Requires: playwright, beautifulsoup4, lxml

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys, json, os
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
    "Informatica": ["https://informatica.gr8people.com/jobs", "https://www.informatica.com/us/careers.html"],
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
    "Pentaho": ["https://www.hitachivantara.com/en-us/company/careers/job-search","https://www.hitachivantara.com/en-us/careers.html"],
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
    # Added large noisy portals you asked to include
    "Salesforce": ["https://careers.salesforce.com/en/jobs/"],
    "Amazon": ["https://www.amazon.jobs/en/"],  # broad; internal allowlist used
    "IBM": ["https://www.ibm.com/careers/search"],
    "SAP": ["https://jobs.sap.com/"],
    # keep the rest as-is
}

PAGE_NAV_TIMEOUT = 40000
PAGE_DOM_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.18
MAX_DETAIL_PAGES = 12000

# Patterns and tokens
IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg|webp)$", re.I)

# FORBIDDEN_WORDS and FORBIDDEN_RE are kept as they are used by clean_title and should_drop_by_title
FORBIDDEN_WORDS = [
    "privacy","privacy policy","about","legal","terms","cookie","cookies",
    "press","blog","partners","pricing","docs","documentation","support",
    "events","resources","login","signin","register","apply now","careers home",
    "read more","create alert","download","whitepaper","product","guide","solutions",
    "press release","case study","blog-post","insights","webinar","newsletter"
]
FORBIDDEN_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in FORBIDDEN_WORDS) + r')\b', re.I)

LOC_TOKENS = [
    "remote","hybrid","usa","united states","uk","united kingdom","germany","france","canada",
    "london","new york","singapore","bengaluru","bangalore","chennai","berlin","paris","india",
    "amsterdam","toronto","zurich","dublin","stockholm","oslo","helsinki","australia","brazil","munich","frankfurt"
]
LOC_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LOC_TOKENS) + r')\b', re.I)

# ROLE_WORDS and ROLE_WORDS_RE are kept as they are used by the should_drop_by_title function.
ROLE_WORDS = [
    "engineer","developer","manager","director","architect","scientist","analyst","product","designer",
    "sales","account","consultant","sre","qa","support","finance","marketing","operations","intern","student",
    "data","devops","infrastructure","research","security"
]
ROLE_WORDS_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in ROLE_WORDS) + r')\b', re.I)


COMPANY_SKIP_RULES = {
    "Ataccama": [r'one-team', r'blog', r'about'],
    "Fivetran": [r'launchers', r'product', r'developer-relations'],
    "Datadog": [r'resources', r'events', r'americas', r'emea', r'learning'],
    "BigEye": [r'product', r'resources', r'product'],
    "Precisely": [r'developer.portal', r'developer'],
    "Qlik": [r'dashboard']
}

CRITICAL_COMPANIES = ["fivetran","ataccama","datadog","snowflake","matillion","oracle","mongodb","databricks"]

# ---------- RELEVANCY LAYER (Option C: Hybrid) ----------
# Points:
# +3 hard-match data/etl/ml/cloud/observability/governance/warehouse
# +2 engineering/platform/devops/sre/back-end
# +1 product/solutions/ai terms/company-allowlist
# -5 strong irrelevance (HR/legal/finance)
# -3 support/customer-success/account-management

RELEVANCE_HARD = [
    r'\bdata engineer\b', r'\bdata-engineer\b', r'\bdata engineering\b',
    r'\bdata platform\b', r'\betl\b', r'\belt\b', r'\belt/elt\b', r'\belt/etl\b',
    r'\bel t\b', r'\bextract\b', r'\bintegrat', r'\bconnector', r'\bconnector(s)?\b',
    r'\bingest', r'\bpipeline\b', r'\bdata pipeline\b', r'\bstream(ing)?\b',
    r'\bkafka\b', r'\bdatabricks\b', r'\bsnowflake\b', r'\bbigquery\b', r'\bredshift\b',
    r'\bwarehouse\b', r'\bmlops\b', r'\bmachine learning\b', r'\bml\b', r'\bai\b',
    r'\bobservab', r'\bobservability\b', r'\bdata quality\b', r'\bgovernance\b', r'\blineage\b',
    r'\bmetadata\b', r'\bcatalog\b', r'\bplatform\b', r'\bcloud\b', r'\baws\b', r'\bgcp\b', r'\bazure\b'
]

RELEVANCE_ENGINEERING = [
    r'\bengineer\b', r'\bdevops\b', r'\bsre\b', r'\bsystem(s)? engineer\b', r'\bsoftware engineer\b',
    r'\bbackend\b', r'\bfull[- ]stack\b', r'\bdeveloper\b', r'\barchitect\b', r'\binfrastructure\b'
]

RELEVANCE_SOFT = [
    r'\bproduct manager\b', r'\bproduct\b', r'\bsolutions\b', r'\bsolution engineer\b',
    r'\bai\b', r'\br&d\b', r'\bresearch\b'
]

IRRELEVANT_STRONG = [
    r'\b(human resources|hr|people operations|people ops|recruit(ment|er)|talent acquisition)\b',
    r'\b(finance|accounting|payroll|tax|legal|compliance specialist)\b',
    r'\b(legal counsel|attorney|paralegal)\b'
]

IRRELEVANT_SOFT = [
    r'\b(customer success|customer success manager|customer support|technical support|support engineer)\b',
    r'\b(account manager|account executive|sales rep)\b',
    r'\b(facilities|office manager|administrative assistant)\b'
]

# company-specific allowlist tokens (noisy portals)
COMPANY_ALLOWLIST = {
    "oracle": ["autonomous", "oracle cloud", "autonomous database", "oci", "exadata"],
    "sap": ["hana", "sap hana", "sap cloud", "sap hana cloud"],
    "ibm": ["watson", "cloud pak", "ibm watson", "ibm cloud"],
    "amazon": ["aws", "amazon web services", "amazon aurora", "redshift", "kinesis"],
    "salesforce": ["einstein", "salesforce", "tableau", "mulesoft"],
}

RELEVANCY_THRESHOLD = 2  # >=2 considered relevant

def _match_any(patterns, text):
    if not text:
        return False
    for p in patterns:
        if re.search(p, text, re.I):
            return True
    return False

def score_title_desc(title, desc, company=""):
    """
    Return a numeric score based on title and desc (both strings). Company is used for allowlist token.
    """
    t = (title or "") + " " + (desc or "")
    t = t.lower()
    score = 0
    # hard matches
    for p in RELEVANCE_HARD:
        if re.search(p, t):
            score += 3
    # engineering
    for p in RELEVANCE_ENGINEERING:
        if re.search(p, t):
            score += 2
    # soft matches
    for p in RELEVANCE_SOFT:
        if re.search(p, t):
            score += 1
    # negative strong
    for p in IRRELEVANT_STRONG:
        if re.search(p, t):
            score -= 5
    # negative soft
    for p in IRRELEVANT_SOFT:
        if re.search(p, t):
            score -= 3
    # company allowlist
    if company:
        lowc = company.lower()
        for key, tokens in COMPANY_ALLOWLIST.items():
            if key in lowc:
                for tok in tokens:
                    if tok in t:
                        score += 1
                        break
    return score

# ---------- END RELEVANCY LAYER ----------

# month map for text date parsing
_MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12
}

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

def _clean_trailing_junk(t):
    if not t:
        return t
    t = re.sub(r'\s*[-|–—|]+\s*$', '', t).strip()
    t = re.sub(r'\s*[-|–—]\s*[A-Z][A-Za-z\s&\-()\.]{2,}$', '', t)
    t = re.sub(r'\((?:remote|hybrid|[A-Za-z0-9\s,.-]+)\)\s*$', '', t, flags=re.I).strip()
    t = re.sub(r'(apply now|read more|view job|learn more|create alert|download your copy)$', '', t, flags=re.I).strip()
    return t

def clean_title(raw):
    if not raw:
        return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = re.sub(r'learn\s*more.*', '', t, flags=re.I)
    t = FORBIDDEN_RE.sub('', t)
    t = re.sub(r'\s+at\s+[A-Z][A-Za-z0-9&\-\s]+$', '', t)
    t = _clean_trailing_junk(t)
    t = re.sub(r'^[\-\•\*]\s*', '', t)
    t = re.sub(r'^\d+\.\s*', '', t)
    t = re.sub(r'\s{2,}', ' ', t).strip(" -:,.|")
    # reduce weird long titles by limiting to 200 chars
    if len(t) > 240:
        t = t[:240] + "..."
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

def _try_parse_posted_on_month_day_year(text):
    m = re.search(r'(?:posted(?: on|:)?\s*)([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})', text, re.I)
    if m:
        mon = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3))
        mm = _MONTH_MAP.get(mon[:3])
        if mm:
            try:
                return date(year, mm, day).isoformat()
            except:
                return ""
    # other pattern: "Posted: Jan 2nd, 2024" or "Posted 2 Jan 2024"
    m2 = re.search(r'posted[:\s]*\s*(\d{1,2})\s*([A-Za-z]+)\s*,?\s*(\d{4})', text, re.I)
    if m2:
        day = int(m2.group(1))
        mon = m2.group(2).lower()[:3]
        year = int(m2.group(3))
        mm = _MONTH_MAP.get(mon)
        if mm:
            try:
                return date(year, mm, day).isoformat()
            except:
                return ""
    return ""

def extract_date_from_html(html_text):
    if not html_text:
        return ""
    # structured JSON-LD datePosted
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        return _iso_only_date(m.group(1))
    m2 = re.search(r'"postedOn"\s*:\s*"([^"]+)"', html_text)
    if m2:
        return _iso_only_date(m2.group(1))
    m3 = re.search(r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I)
    if m3:
        return _iso_only_date(m3.group(1))
    m4 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m4:
        return _iso_only_date(m4.group(1))
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        days = int(mm.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    parsed = _try_parse_posted_on_month_day_year(html_text)
    if parsed:
        return parsed
    mm2 = re.search(r'(\d{4}-\d{2}-\d{2})', html_text)
    if mm2:
        return mm2.group(1)
    return ""

# ----------------------------------------------------------------------
# UPDATED FUNCTION is_likely_job_anchor
# ----------------------------------------------------------------------
def is_likely_job_anchor(href, text):
    if not href:
        return False

    h = href.lower()
    t = (text or "").lower()

    # Reject clearly non-job sections
    BAD = [
        "about", "privacy", "security", "press",
        "events", "culture", "life-at", "team",
        "leadership", "story", "product", "solutions",
        "resources", "download", "company", "blog"
    ]
    if any(b in h for b in BAD):
        return False
    if any(b in t for b in BAD):
        return False

    # Accept only REAL ATS links
    ATS = [
        "lever.co",
        "greenhouse",
        "myworkdayjobs",
        "ashbyhq",
        "bamboohr",
        "smartrecruiters",
        "jobvite",
        "/jobs/",
        "/job/",
        "fivetran.com",
        "fivetran"
    ]
    if any(a in h for a in ATS):
        return True

    # Role keyword requirement
    ROLE = [
        "engineer","developer","manager","director","architect",
        "scientist","analyst","product","designer","sales",
        "account","consultant","sre","qa","support",
        "data","devops","intern","student"
    ]
    if any(r in t for r in ROLE):
        return True

    return False
# ----------------------------------------------------------------------

def extract_location_from_text(txt):
    if not txt:
        return "", ""
    s = txt.replace("\r", " ").replace("\n", " ").strip()
    # parenthetical location at end
    paren = re.search(r"\(([^)]+)\)\s*$", s)
    if paren:
        loc = paren.group(1)
        title = s[: paren.start()].strip(" -:,")
        return title, normalize_location(loc)
    # split on separators, if last looks like location
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
        ".location",".job-location","span[class*='location']",".posting-location",
        ".job_meta_location",".location--name",".job-card__location",".opening__meta",
        ".job-meta__location",".opening__location",".jobCard-location",".locationTag"
    ]
    def attr_location(tag):
        if not tag:
            return ""
        for attr in ("data-location","data-geo","aria-label","title","data-qa-location","data-test-location"):
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

# ---------- SENIORITY DETECTION ----------
def detect_seniority(title):
    if not title:
        return "Unknown"
    t = title.lower()
    # Director / Exec
    if any(x in t for x in ["chief ", "cxo", "cto", "ceo", "cfo", "coo", "vp ", "vice president", "svp", "evp", "executive director", "head of", "director", "managing director"]):
        return "Director+"
    if any(x in t for x in ["principal", "distinguished", "fellow"]):
        return "Principal/Staff"
    if any(x in t for x in ["senior", "sr.", "sr ", "lead ", "lead-", "team lead", "senior engineer", "senior manager"]):
        return "Senior"
    if any(x in t for x in ["manager", "mgr", "management", "people manager", "engineering manager"]):
        return "Manager"
    if any(x in t for x in ["mid ", "mid-", "intermediate", "experience", "level ii", "ii ", "2 ", "associate", "regular", "software engineer ii", "engineer ii"]):
        return "Mid"
    if any(x in t for x in ["junior", "jr.", "jr ", "entry", "graduate", "fresher"]):
        return "Entry"
    if any(x in t for x in ["intern", "internship", "working student", "werkstudent"]):
        return "Intern"
    return "Unknown"

# ---------- SCRAPE ----------
# PATCH 1: Replace fetch_page_content with SPA-aware version
def fetch_page_content(page, url, nav_timeout=45000, dom_timeout=15000):
    try:
        # Load full SPA content (React)
        page.goto(url, timeout=nav_timeout, wait_until="networkidle")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1200)  # Let React hydrate
        return page.content()
    except Exception as e:
        print(f"[WARN] SPA load failed {url}: {e}")
        # fallback: domcontentloaded
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)
            return page.content()
        except Exception as e2:
            print(f"[WARN] fallback failed {url}: {e2}")
            return ""

def should_drop_by_title(title):
    # after cleaning, if title lacks role words, drop it
    if not title or len(title.strip()) == 0:
        return True
    low = title.lower()
    if ROLE_WORDS_RE.search(low):
        return False
    # allow short 'intern' etc
    if re.search(r'\bintern\b', low):
        return False
    return True

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
                # anchors
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    text = a.get_text(" ", strip=True) or ""
                    href_abs = normalize_link(main_url, href)
                    if is_likely_job_anchor(href_abs, text):
                        candidates.append((href_abs, text, a))

                # PATCH 2: FIVETRAN SPECIAL JOB EXTRACTOR
                if "fivetran.com" in main_url:
                    print("[FIVETRAN] Running React job-card extractor")

                    # Find React job cards
                    for job in soup.select("div[data-job-id], div.job-card, a[data-job-id]"):
                        text = job.get_text(" ", strip=True)

                        # Extract job link
                        link = (
                            job.get("href")
                            or job.get("data-url")
                            or job.get("data-job-url")
                        )

                        # If only job-id exists
                        if not link:
                            jid = job.get("data-job-id")
                            if jid:
                                link = f"https://www.fivetran.com/careers/job/{jid}"

                        if not link:
                            continue

                        link = normalize_link(main_url, link)

                        if text.strip():
                            candidates.append((link, text, job))
                            print(f"[FIVETRAN] Found job: {text} -> {link}")

                # job card containers
                for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"):
                    a = el.find("a", href=True)
                    text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text, el))
                # try iframe to ATS if none
                if not candidates:
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse","lever","myworkday","bamboohr","ashby","jobs.lever","jobvite")):
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
                # dedupe + company skip rules
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
                        if skip: break
                    if skip:
                        continue
                    filtered.append((href, text, el))
                # parse filtered candidates
                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    title_candidate = anchor_text or ""
                    title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    if not title_clean:
                        title_clean = clean_title(title_candidate)
                    else:
                        title_clean = clean_title(title_clean)
                    # prefer card location
                    card_loc = try_extract_location_from_card(el)
                    if card_loc and not location_candidate:
                        location_candidate = card_loc

                    # --- RELEVANCY: Option C hybrid flow ---
                    # 1) light score on anchor/title
                    light_score = score_title_desc(title_candidate, "", company)
                    # Decide whether to fetch detail:
                    # - If must_detail is True (original reasons) we will still fetch detail; else
                    # - If light_score >= threshold => accept without fetching detail
                    # - If 0 < light_score < threshold => ambiguous => fetch detail and rescore
                    # - If light_score <= 0 => drop early (unless forced to detail by existing must_detail rules)
                    posting_date = ""
                    must_detail = False
                    must_reasons = []

                    # original must_detail logic kept
                    if company.lower() in CRITICAL_COMPANIES:
                        must_detail = True; must_reasons.append("critical_company")
                    if not location_candidate:
                        must_detail = True; must_reasons.append("no_location")
                    if not posting_date:
                        must_detail = True; must_reasons.append("no_posting_date")
                    if len((title_clean or "").split()) < 2:
                        must_detail = True; must_reasons.append("short_title")
                    if LOC_RE.search(title_candidate):
                        must_detail = True; must_reasons.append("title_contains_loc_token")
                    if any(x in (link or "").lower() for x in ["/job/","/jobs/","greenhouse","lever.co","ashbyhq","bamboohr","myworkdayjobs","gr8people","welcometothejungle","career","jobvite","smartrecruiters"]):
                        must_detail = True; must_reasons.append("link_looks_like_ats")

                    # Logging decision
                    if must_detail:
                        print(f"[DETAIL_DECISION] (forced) company={company} link={link} reasons={','.join(must_reasons)} detail_count={detail_count}")
                    else:
                        print(f"[LIGHT_SCORE] company={company} link={link} title='{title_candidate[:80]}' light_score={light_score}")

                    detail_html = ""
                    s = None
                    final_score = light_score

                    # If light_score >= threshold and not forced, skip detail fetch
                    if light_score >= RELEVANCY_THRESHOLD and not must_detail:
                        # Accept based on light score; final_score remains light_score
                        keep_reason = f"light_score_ok({light_score})"
                        print(f"[KEEP-LIGHT] {company} | {title_candidate} | score={light_score}")
                    else:
                        # If light_score <= 0 and not forced: drop early
                        if light_score <= 0 and not must_detail:
                            # Drop early
                            print(f"[DROP-LIGHT] Dropping by light_score <=0 -> company={company} title='{title_candidate}' score={light_score}")
                            continue
                        # Otherwise fetch detail and compute final score
                        if detail_count < MAX_DETAIL_PAGES:
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
                                    for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--name", ".opening__meta", ".job-card__location", ".posting__location"]:
                                        eloc = s.select_one(sel)
                                        if eloc and eloc.get_text(strip=True):
                                            found_loc = normalize_location(eloc.get_text(" ", strip=True)); break
                                    if found_loc:
                                        location_candidate = found_loc; print(f"[DETAIL_LOC] found -> {location_candidate}")
                                    # JSON-LD robust parsing (jobLocation / datePosted)
                                    for script in s.find_all("script", type="application/ld+json"):
                                        text = script.string
                                        if not text: continue
                                        payload = None
                                        for attempt in (text, "[" + text + "]"):
                                            try:
                                                payload = json.loads(attempt); break
                                            except:
                                                cleaned = re.sub(r'[\x00-\x1f]+', ' ', text)
                                                try:
                                                    payload = json.loads(cleaned); break
                                                except:
                                                    payload = None
                                        if not payload: continue
                                        items = payload if isinstance(payload, list) else [payload]
                                        for item in items:
                                            if not isinstance(item, dict): continue
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
                                                            if v: parts.append(str(v))
                                                        if parts:
                                                            location_candidate = normalize_location(", ".join(parts)); print(f"[DETAIL_JSON_LOC] found -> {location_candidate}"); break
                                                    if jl_entry.get("name"):
                                                        location_candidate = normalize_location(str(jl_entry.get("name"))); print(f"[DETAIL_JSON_LOC] found -> {location_candidate}"); break
                                                elif isinstance(jl_entry, str):
                                                    location_candidate = normalize_location(jl_entry); print(f"[DETAIL_JSON_LOC] found -> {location_candidate}"); break
                                            # datePosted parsing
                                            if isinstance(item.get("datePosted"), str) and not posting_date:
                                                posting_date = _iso_only_date(item.get("datePosted")); print(f"[DETAIL_DATE] found -> {posting_date}")
                                    # EXTENDED ATS-specific parsing (kept identical to previous)
                                    if not posting_date:
                                        try:
                                            text_blob = detail_html
                                            m = re.search(r'window\.__WD_DATA__\s*=\s*({.+?});', text_blob, re.S)
                                            if m:
                                                try:
                                                    wd = json.loads(m.group(1))
                                                    def wd_find_date(obj):
                                                        if isinstance(obj, dict):
                                                            for k,v in obj.items():
                                                                if isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}', v): return v
                                                                res = wd_find_date(v);
                                                                if res: return res
                                                        if isinstance(obj, list):
                                                            for it in obj:
                                                                res = wd_find_date(it)
                                                                if res: return res
                                                        return None
                                                    res = wd_find_date(wd)
                                                    if res:
                                                        posting_date = _iso_only_date(res); print(f"[DETAIL_AUX_DATE] Workday -> {posting_date}")
                                                except:
                                                    pass
                                            if not posting_date:
                                                m2 = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?});', text_blob, re.S)
                                                if m2:
                                                    try:
                                                        st = json.loads(m2.group(1))
                                                        for key in ("job","jobPosting","job_posting"):
                                                            node = st.get(key) if isinstance(st, dict) else None
                                                            if isinstance(node, dict):
                                                                for k in ("posted_at","updated_at","created_at","date_posted","date"):
                                                                    if node.get(k):
                                                                        posting_date = _iso_only_date(str(node.get(k))); print(f"[DETAIL_AUX_DATE] Greenhouse->{k} -> {posting_date}"); break
                                                                if posting_date: break
                                                    except:
                                                        pass
                                            if not posting_date:
                                                m3 = re.search(r'({"jobPosting".+?})', text_blob, re.S) or re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?});', text_blob, re.S)
                                                if m3:
                                                    try:
                                                        payload = json.loads(m3.group(1))
                                                        def find_lever_date(o):
                                                            if isinstance(o, dict):
                                                                for k,v in o.items():
                                                                    if k.lower() in ("createdat","created_at","postingdate","postedat","post_date") and isinstance(v,str):
                                                                        return v
                                                                    res = find_lever_date(v)
                                                                    if res: return res
                                                            if isinstance(o, list):
                                                                for it in o:
                                                                    res = find_lever_date(it)
                                                                    if res: return res
                                                            return None
                                                        res = find_lever_date(payload)
                                                        if res:
                                                            posting_date = _iso_only_date(res); print(f"[DETAIL_AUX_DATE] Lever -> {posting_date}")
                                                    except:
                                                        pass
                                            if not posting_date:
                                                m4 = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>', text_blob, re.S)
                                                if m4:
                                                    try:
                                                        nd = json.loads(m4.group(1))
                                                        def nd_find_date(obj):
                                                            if isinstance(obj, dict):
                                                                for v in obj.values():
                                                                    if isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}', v): return v
                                                                    res = nd_find_date(v)
                                                                    if res: return res
                                                            if isinstance(obj, list):
                                                                for it in obj:
                                                                    res = nd_find_date(it)
                                                                    if res: return res
                                                            return None
                                                        res = nd_find_date(nd)
                                                        if res:
                                                            posting_date = _iso_only_date(res); print(f"[DETAIL_AUX_DATE] NextJS -> {posting_date}")
                                                    except:
                                                        pass
                                            if not posting_date:
                                                for script in s.find_all("script"):
                                                    t = script.string or script.text or ""
                                                    if not t: continue
                                                    for key in ("posted_at","post_date","date_posted","datePosted","date_published","created_at","createdAt"):
                                                        mkey = re.search(rf'"{key}"\s*:\s*"([^"]+)"', t, re.I)
                                                        if mkey:
                                                            posting_date = _iso_only_date(mkey.group(1)); print(f"[DETAIL_AUX_DATE] script-key {key} -> {posting_date}"); break
                                                    if posting_date: break
                                            if not posting_date:
                                                mm = re.search(r'(\d{4}-\d{2}-\d{2})', detail_html)
                                                if mm:
                                                    posting_date = _iso_only_date(mm.group(1)); print(f"[DETAIL_AUX_DATE] ISO fallback -> {posting_date}")
                                            if not posting_date:
                                                parsed = _try_parse_posted_on_month_day_year(detail_html)
                                                if parsed:
                                                    posting_date = parsed; print(f"[DETAIL_AUX_DATE] PostedOnMonth -> {posting_date}")
                                        except Exception as e:
                                            print(f"[WARN] extended-ats-date extractor failed: {e}")
                                    # DATE NEAR TITLE heuristic
                                    if not posting_date and s:
                                        try:
                                            h1 = s.find("h1")
                                            if h1:
                                                h1_block = h1.get_text(" ", strip=True)
                                                parent_block = h1.parent.get_text(" ", strip=True) if h1.parent else ""
                                                combined = h1_block + " " + parent_block
                                                m_iso = re.search(r'(\d{4}-\d{2}-\d{2})', combined)
                                                if m_iso:
                                                    posting_date = _iso_only_date(m_iso.group(1)); print(f"[TITLE_DATE] ISO near title -> {posting_date}")
                                                if not posting_date:
                                                    m_days = re.search(r'posted\s+(\d+)\s+days?\s+ago', combined, re.I)
                                                    if m_days:
                                                        d = date.today() - timedelta(days=int(m_days.group(1)))
                                                        posting_date = d.isoformat(); print(f"[TITLE_DATE] days-ago near title -> {posting_date}")
                                        except Exception as e:
                                            print(f"[WARN] title-date-extractor error: {e}")
                                    # STRICT ISO near keywords (Patch 1)
                                    if not posting_date:
                                        snippet = detail_html[:50000]
                                        m = re.search(r'(?i)(posted|created|updated|date|time)[^0-9]{0,80}(\d{4}-\d{2}-\d{2})', snippet)
                                        if m:
                                            posting_date = _iso_only_date(m.group(2)); print(f"[STRICT_ISO_PATCH1] -> {posting_date}")
                                    # final fallback helper
                                    if not posting_date:
                                        found_date = extract_date_from_html(detail_html)
                                        if found_date:
                                            posting_date = found_date; print(f"[DETAIL_DATE_FALLBACK_PATCH1] -> {posting_date}")
                                except Exception as e:
                                    print(f"[WARN] detail parse fail {link} -> {e}")
                            else:
                                print(f"[WARN] empty detail_html for {link}")

                        # Recompute final_score using title_clean and detail_html
                        final_score = score_title_desc(title_clean or title_candidate, detail_html or "", company)
                        print(f"[FINAL_SCORE] company={company} link={link} final_score={final_score} (light={light_score})")

                        if final_score >= RELEVANCY_THRESHOLD:
                            keep_reason = f"final_score_ok({final_score})"
                            print(f"[KEEP-FINAL] {company} | {title_clean or title_candidate} | score={final_score}")
                        else:
                            print(f"[DROP-FINAL] Dropping after detail fetch -> score={final_score} | company={company} | title='{title_clean or title_candidate}'")
                            continue

                    # final normalization (unchanged)
                    title_final = clean_title(title_clean) if title_clean else clean_title(anchor_text or "")
                    location_candidate = normalize_location(location_candidate)
                    posting_date_final = posting_date or ""
                    # deep location extraction if still empty
                    if not location_candidate and detail_html:
                        try:
                            m = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', detail_html)
                            if m:
                                city = m.group(1)
                                m2 = re.search(r'"addressCountry"\s*:\s*"([^"]+)"', detail_html)
                                country = m2.group(1) if m2 else ""
                                loc = f"{city}, {country}".strip(", ")
                                if loc:
                                    location_candidate = normalize_location(loc); print(f"[DEEP_LOC] Workday -> {location_candidate}")
                            if not location_candidate:
                                m = re.search(r'"additionalLocations"\s*:\s*\[(.+?)\]', detail_html, re.S)
                                if m:
                                    locs_raw = m.group(1)
                                    locs = re.findall(r'"([^"]+)"', locs_raw)
                                    if locs:
                                        location_candidate = normalize_location(locs[0]); print(f"[DEEP_LOC] Greenhouse additionalLocations -> {location_candidate}")
                            if not location_candidate:
                                m = re.search(r'"categories"\s*:\s*{[^}]*"location"\s*:\s*"([^"]+)"', detail_html)
                                if m:
                                    location_candidate = normalize_location(m.group(1)); print(f"[DEEP_LOC] Lever -> {location_candidate}")
                            if not location_candidate:
                                m = re.search(r'"locations"\s*:\s*\[(.+?)\]', detail_html, re.S)
                                if m:
                                    js = m.group(1)
                                    city = re.search(r'"city"\s*:\s*"([^"]+)"', js)
                                    region = re.search(r'"region"\s*:\s*"([^"]+)"', js)
                                    country = re.search(r'"country"\s*:\s*"([^"]+)"', js)
                                    parts = [x.group(1) for x in (city, region, country) if x]
                                    if parts:
                                        location_candidate = normalize_location(", ".join(parts)); print(f"[DEEP_LOC] Ashby -> {location_candidate}")
                        except Exception as e:
                            print(f"[WARN] deep-loc extractor error: {e}")
                    # ULTRA_LOC regex (Patch 2)
                    if not location_candidate and detail_html:
                        snippet = detail_html[:40000]
                        mm = re.search(r'([A-Z][a-zA-Z]+)[,\s\-–]+(USA|United States|UK|Germany|France|India|Singapore|Canada|Australia)', snippet)
                        if mm:
                            location_candidate = normalize_location(mm.group(0)); print(f"[ULTRA_LOC_PATCH2] -> {location_candidate}")
                    location_final = normalize_location(location_candidate)
                    # fallback location from link path
                    if not location_final:
                        mloc = re.search(r'/(remote|new[-_]york|london|berlin|singapore|bengaluru|chennai|munich|frankfurt)[/\-]?', link or "", re.I)
                        if mloc:
                            location_final = mloc.group(1).replace('-', ' ').title()
                    # fallback posting date from anchor
                    if not posting_date_final:
                        posted_from_anchor = re.search(r'posted\s+(\d+)\s+days?\s+ago', anchor_text or "", re.I)
                        if posted_from_anchor:
                            d = date.today() - timedelta(days=int(posted_from_anchor.group(1)))
                            posting_date_final = d.isoformat()
                    # final pre-filter: drop obvious marketing / product / non-job titles
                    if not should_drop_by_title(title_final):
                        rows.append({
                            "Company": company,
                            "Job Title": title_final,
                            "Job Link": link,
                            "Location": location_final,
                            "Posting Date": posting_date_final,
                            "Days Since Posted": ""
                        })
                    else:
                        print(f"[DROP] Dropping non-job/marketing row -> {company} | {title_final} | {link}")
        browser.close()
        return rows

# --- MAIN EXECUTION LOGIC ---
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
                # prefer row with posting date if duplicate
                existing = dedup[lk]
                if not existing.get("Posting Date") and r.get("Posting Date"):
                    dedup[lk] = r
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

        # PATCH (GEMINI-ready): Always write to repo root
        repo_root = os.path.dirname(os.path.abspath(__file__))
        outfile = os.path.join(repo_root, "jobs_final_hard.csv")

        fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Seniority"]
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in out_sorted:
                row_to_write = {k: (v if v is not None else "") for k, v in r.items() if k in fieldnames}
                for k in fieldnames:
                    if k not in row_to_write:
                        row_to_write[k] = ""
                writer.writerow(row_to_write)
        print(f"[OK] wrote {len(out_sorted)} rows -> {outfile}")
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during execution: {e}")
        sys.exit(1)
