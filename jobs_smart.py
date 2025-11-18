# jobs_smart.py (Option C)
# Playwright + BeautifulSoup hybrid scraper with safer detail fetch and ATS-aware extraction
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys, json
from datetime import datetime, date, timedelta

# ------------------ CONFIG ------------------
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
# timeouts (ms)
PAGE_NAV_TIMEOUT = 40000
PAGE_DOM_TIMEOUT = 7000     # reduced detailed fetch timeout
PAGE_IDLE_TIMEOUT = 5000
SLEEP_BETWEEN_REQUESTS = 0.18

IMAGE_EXT = re.compile(r"\.(jpg|jpeg|png|gif|svg|webp)$", re.I)

FORBIDDEN = [
    "privacy", "privacy policy", "company", "about", "legal", "terms",
    "cookie", "cookies", "resources", "blog", "press", "partners", "pricing",
    "documentation", "docs", "contact", "subscribe", "newsletter", "faq",
]
FORBIDDEN_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in FORBIDDEN) + r')\b', re.I)

LOC_TOKENS = [
    "remote", "hybrid", "usa", "united states", "united kingdom", "uk", "germany",
    "france", "canada", "london", "new york", "singapore", "bengaluru", "bangalore",
    "chennai", "berlin", "paris", "india", "amsterdam", "toronto", "zurich",
    "dublin", "stockholm", "oslo", "helsinki", "australia", "brazil", "sao paulo"
]
LOC_RE = re.compile(r'\b(?:' + '|'.join(re.escape(x) for x in LOC_TOKENS) + r')\b', re.I)

ROLE_WORDS_RE = re.compile(r'\b(engineer|developer|analyst|manager|director|product|data|scientist|architect|consultant|sales|designer|sre|qa|engineering|specialist)\b', re.I)

# ------------------ HELPERS ------------------
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
    parts = [p.strip() for p in re.split(r'[,/]+', s) if p.strip()]
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            # Normalize common forms
            if key == "remote":
                out.append("Remote")
            else:
                out.append(p.title())
    return ", ".join(out)

def extract_date_from_html(html_text):
    if not html_text:
        return ""
    # JSON-LD datePosted
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        return _iso_only_date(m.group(1))
    # Workday "postedOn"
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
        days = int(mm.group(1))
        return (date.today() - timedelta(days=days)).isoformat()
    # Greenhouse / Ashby fallback: look for date formats like YYYY-MM-DD
    mm2 = re.search(r'(\d{4}-\d{2}-\d{2})', html_text)
    if mm2:
        return mm2.group(1)
    return ""

def _iso_only_date(s):
    try:
        # attempt to parse ISO-ish timestamps
        return datetime.fromisoformat(s.split("T")[0]).date().isoformat()
    except:
        try:
            return datetime.strptime(s.split("T")[0], "%Y-%m-%d").date().isoformat()
        except:
            return s.split("T")[0]

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
                 'gr8people', 'boards.greenhouse', 'job-boards']
    if href and any(p in href.lower() for p in positives):
        return True
    if any(p in low for p in positives):
        return True
    if text and ROLE_WORDS_RE.search(text) and len(text) < 140:
        return True
    return False

def extract_location_from_text(txt):
    if not txt:
        return "", ""
    s = txt.replace("\r", " ").replace("\n", " ").strip()
    # parentheses at end -> often location like "(NYC, Remote)"
    paren = re.search(r'\(([^)]+)\)\s*$', s)
    if paren:
        loc = paren.group(1)
        title = s[:paren.start()].strip(" -:,")
        return title, normalize_location(loc)
    # split heuristics
    parts = re.split(r'\s{2,}| - | — | – | \| |·|•| — |,', s)
    parts = [p.strip() for p in parts if p and p.strip()]
    possible_location = ""
    possible_title = s
    if len(parts) >= 2:
        last = parts[-1]
        if LOC_RE.search(last) or (len(last.split()) <= 4 and re.search(r'[A-Za-z]', last)):
            possible_location = last
            possible_title = " ".join(parts[:-1])
    if not possible_location:
        m = LOC_RE.search(s)
        if m:
            idx = m.start()
            candidate = s[idx:].strip(" -,:;")
            candidate = re.split(r'\s{2,}| - | — | – | \| ', candidate)[0].strip()
            possible_location = candidate
            possible_title = s.replace(candidate, '').strip(" -:,")
    return possible_title, normalize_location(possible_location)

# ------------------ PARSING helpers ------------------
def parse_listing_for_links(base_url, html):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = (a.get_text(" ", strip=True) or "")
        href_abs = normalize_link(base_url, href)
        if is_likely_job_anchor(href_abs, text):
            candidates.append((href_abs, text, a))
    # find explicit job-card containers & try to attach location metadata
    for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"):
        # anchor inside card
        a = el.find("a", href=True)
        text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
        href = normalize_link(base_url, a.get("href")) if a else ""
        if is_likely_job_anchor(href, text):
            candidates.append((href, text, el))
    # dedupe preserving order
    seen = set()
    out = []
    for href, text, el in candidates:
        if not href:
            continue
        if href.rstrip("/") == base_url.rstrip("/"):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append((href, text, el))
    return out

def try_extract_location_from_card(el):
    # el may be BeautifulSoup Tag or anchor element
    if not el:
        return ""
    # if it's a Tag representing the card, search for location selectors
    search_selectors = [
        ".location", ".job-location", "span[class*='location']", ".posting-location",
        ".job_meta_location", ".location--name", ".job-card__location", ".opening__meta"
    ]
    # if el is Tag, use select_one; if it's an <a> element, check parent nodes
    tag = el
    if el.name == 'a':
        # check parent containers up to 3 levels
        parent = el.parent
        depth = 0
        while parent and depth < 4:
            for sel in search_selectors:
                found = parent.select_one(sel)
                if found and found.get_text(strip=True):
                    return normalize_location(found.get_text(" ", strip=True))
            parent = parent.parent
            depth += 1
    else:
        for sel in search_selectors:
            found = tag.select_one(sel)
            if found and found.get_text(strip=True):
                return normalize_location(found.get_text(" ", strip=True))
    return ""

def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, dom_timeout=PAGE_DOM_TIMEOUT, wait_dom=True):
    try:
        # prefer DOMContentLoaded to avoid networkidle stalls
        if wait_dom:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
        else:
            page.goto(url, timeout=nav_timeout)
        # small wait to allow minimal dynamic content
        page.wait_for_timeout(300)  # 300ms
        return page.content()
    except PWTimeout:
        try:
            page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
            return page.content()
        except Exception as e:
            print(f"[WARN] timeout/fetch failed: {url} -> {e}")
            return ""
    except Exception as e:
        print(f"[WARN] fetch failed: {url} -> {e}")
        return ""

# ------------------ MAIN SCRAPE ------------------
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

                candidates = parse_listing_for_links(main_url, listing_html)

                # if still nothing, check if there's an iframe to ATS and try it
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse", "lever", "myworkday", "bamboohr", "ashby", "job-boards", "jobs.lever")):
                            src_full = normalize_link(main_url, src)
                            print(f"[INFO] found iframe src -> {src_full} (trying)")
                            iframe_html = fetch_page_content(page, src_full, wait_dom=True)
                            if iframe_html:
                                candidates += parse_listing_for_links(src_full, iframe_html)

                # final fallback: any anchors (best-effort)
                if not candidates:
                    soup = BeautifulSoup(listing_html, "lxml")
                    for a in soup.find_all("a", href=True):
                        text = a.get_text(" ", strip=True) or ""
                        href = normalize_link(main_url, a.get("href"))
                        if is_likely_job_anchor(href, text):
                            candidates.append((href, text, a))

                # Filter & dedupe
                filtered = []
                for href, text, el in candidates:
                    if IMAGE_EXT.search(href):
                        continue
                    low = (text or href or "").lower()
                    if re.search(r'/product|/features|/pricing|/docs|/resources|/legal|/contact', href, re.I):
                        if not ROLE_WORDS_RE.search(text):
                            continue
                    filtered.append((href, text, el))

                # parse candidates
                for link, anchor_text, el in filtered:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                    title_candidate = anchor_text or ""
                    title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    if not title_clean:
                        title_clean = clean_title(title_candidate)

                    posting_date = ""
                    need_detail = (not location_candidate) or (len(title_clean) < 3)

                    # try lightweight extraction from card (if available)
                    card_loc = try_extract_location_from_card(el)
                    if card_loc and not location_candidate:
                        location_candidate = card_loc

                    detail_html = ""
                    if need_detail:
                        # limit heavy detail fetches, use dom-only small timeout
                        detail_html = fetch_page_content(page, link, nav_timeout=PAGE_DOM_TIMEOUT*6, dom_timeout=PAGE_DOM_TIMEOUT, wait_dom=True)
                        if detail_html:
                            try:
                                s = BeautifulSoup(detail_html, "lxml")
                                # H1 fallback
                                if (not title_clean or len(title_clean) < 3) and s.find("h1"):
                                    title_clean = clean_title(s.find("h1").get_text(" ", strip=True))
                                # common location selectors
                                for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--name", ".opening__meta", ".job-card__location"]:
                                    eloc = s.select_one(sel)
                                    if eloc and eloc.get_text(strip=True):
                                        location_candidate = normalize_location(eloc.get_text(" ", strip=True))
                                        break
                                # date extraction
                                pd = extract_date_from_html(detail_html)
                                if pd:
                                    posting_date = pd
                                # Greenhouse specific: check meta or time tags
                                if not posting_date:
                                    # try small set of selectors
                                    for sel in ["time", ".posted-date", ".job-posted", ".posting-date", ".opening__posted"]:
                                        t = s.select_one(sel)
                                        if t and t.get("datetime"):
                                            posting_date = _iso_only_date(t.get("datetime"))
                                            break
                                        if t and t.get_text(strip=True):
                                            dt = re.search(r'(\d{4}-\d{2}-\d{2})', t.get_text(" ", strip=True))
                                            if dt:
                                                posting_date = dt.group(1)
                                                break
                            except Exception as e:
                                print(f"[WARN] detail parse fail {link} -> {e}")

                    title_final = clean_title(title_clean) if title_clean else clean_title(anchor_text)
                    location_final = normalize_location(location_candidate)
                    posting_date_final = posting_date or ""

                    # fallback: if location still blank but link contains location token, parse from url
                    if not location_final:
                        mloc = re.search(r'/(remote|new-york|london|berlin|singapore|bengaluru|chennai|munich|frankfurt)[/\-]?', link, re.I)
                        if mloc:
                            location_final = mloc.group(1).replace('-', ' ').title()
                    if not posting_date_final:
                        # attempt posted X days in anchor text
                        posted_from_anchor = re.search(r'posted\s+(\d+)\s+days?\s+ago', anchor_text or "", re.I)
                        if posted_from_anchor:
                            d = date.today() - timedelta(days=int(posted_from_anchor.group(1)))
                            posting_date_final = d.isoformat()

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
