# jobs_smart_cplus.py
# Cleaned + compact Playwright + BeautifulSoup hybrid scraper
# ATS-aware, special extractors, enriched relevancy flow.
# Run: python3 jobs_smart_cplus_final_compact.py
# Requires: playwright, beautifulsoup4, lxml

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, csv, time, sys, json, os
from datetime import datetime, date, timedelta

# import deep site-specific extractors (created separately)
from special_extractors_deep import SPECIAL_EXTRACTORS_DEEP

# ---------- CONFIG ----------
# Still restricted to Cloudera for debugging
COMPANIES = {
    "Cloudera": ["https://cloudera.wd5.myworkdayjobs.com/External_Career"]
}

PAGE_NAV_TIMEOUT = 40000
PAGE_DOM_TIMEOUT = 15000
SLEEP_BETWEEN_REQUESTS = 0.18
MAX_DETAIL_PAGES = 12000

# ---------- PATTERNS ----------
FORBIDDEN_RE = re.compile(
    r'\b(?:privacy|about|press|blog|partners|pricing|docs|support|events|resources|login|apply now|read more)\b', re.I
)

LOC_RE = re.compile(r'\b(?:remote|hybrid|usa|united states|uk|germany|india|london|new york|singapore|berlin|bengaluru)\b', re.I)

ROLE_WORDS_RE = re.compile(r'\b(?:engineer|developer|manager|director|architect|scientist|analyst|product|sre|intern)\b', re.I)

COMPANY_SKIP_RULES = {
    "Ataccama": [r'one-team', r'blog', r'about'],
    "Fivetran": [r'launchers', r'product', r'developer-relations'],
    "Datadog": [r'resources', r'events', r'learning'],
    "BigEye": [r'product', r'resources'],
}

CRITICAL_COMPANIES = {"fivetran","ataccama","datadog","snowflake","matillion","oracle","mongodb","databricks"}

RELEVANCE_HARD = [r'\bdata engineer\b', r'\betl\b', r'\bintegrat', r'\bconnector', r'\bpipeline\b', r'\bsnowflake\b', r'\bdatabricks\b', r'\bobs ervab']
RELEVANCY_THRESHOLD = 2

def score_title_desc(title, desc, company=""):
    t = ((title or "") + " " + (desc or "")).lower()
    score = 0
    for p in RELEVANCE_HARD:
        if re.search(p, t): score += 3
    if company and "oracle" in company.lower() and "autonomous" in t:
        score += 1
    return score

# helpers
def normalize_link(base, href):
    if not href: return ""
    href = href.strip()
    if href.startswith("//"): href = "https:" + href
    parsed = urlparse(href)
    if parsed.netloc: return href
    try: return urljoin(base, href)
    except: return href

def clean_title(raw):
    if not raw: return ""
    t = re.sub(r'\s+', ' ', raw).strip()
    t = FORBIDDEN_RE.sub('', t)
    t = re.sub(r'\((?:remote|hybrid)[^)]+\)\s*$', '', t, flags=re.I).strip()
    t = re.sub(r'^[\-•\*]\s*', '', t)
    return t.strip(" -:,.|")[:240]

def extract_location_from_text(txt):
    if not txt: return "", ""
    s = txt.replace("\r"," ").replace("\n"," ").strip()
    paren = re.search(r'\(([^)]+)\)\s*$', s)
    if paren: return s[:paren.start()].strip(" -:,"), paren.group(1)
    parts = re.split(r'\s{2,}| - | — | – | \| |·|•|,', s)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2 and (LOC_RE.search(parts[-1]) or len(parts[-1].split())<=4):
        return " ".join(parts[:-1]), parts[-1]
    m = LOC_RE.search(s)
    if m:
        idx = m.start(); candidate = s[idx:].strip(" -,:;")
        title = s.replace(candidate,"").strip(" -:,")
        return title, candidate
    return s, ""

def try_extract_location_from_card(el):
    if not el: return ""
    selectors = [".location", ".job-location", ".posting-location", ".job_meta_location"]
    for sel in selectors:
        try:
            found = el.select_one(sel)
        except: found = None
        if found and found.get_text(strip=True):
            return found.get_text(" ", strip=True)
    # attribute fallback
    for attr in ("data-location","data-geo","aria-label","title"):
        v = el.get(attr)
        if v and isinstance(v,str): return v
    return ""

def detect_seniority(title):
    if not title: return "Unknown"
    t = title.lower()
    if any(x in t for x in ["chief ","cto","vp ","director","head of"]): return "Director+"
    if any(x in t for x in ["principal","distinguished"]): return "Principal/Staff"
    if any(x in t for x in ["senior","sr.","lead "]): return "Senior"
    if any(x in t for x in ["manager","mgr"]): return "Manager"
    if any(x in t for x in ["mid ","associate","ii"]): return "Mid"
    if any(x in t for x in ["junior","jr.","entry"]): return "Entry"
    if any(x in t for x in ["intern","internship","werkstudent"]): return "Intern"
    return "Unknown"

def is_likely_job_anchor(href, text):
    if not href: return False
    h = (href or "").lower(); t = (text or "").lower()
    BAD = ["about","privacy","press","events","product","resources","download","company","blog"]
    if any(b in h for b in BAD) or any(b in t for b in BAD): return False
    ATS = ["lever.co","greenhouse","myworkdayjobs","ashby","bamboohr","smartrecruiters","jobvite","/jobs/","/job/","fivetran.com"]
    if any(a in h for a in ATS): return True
    if ROLE_WORDS_RE.search(t): return True
    return False

def fetch_page_content(page, url, nav_timeout=PAGE_NAV_TIMEOUT, dom_timeout=PAGE_DOM_TIMEOUT):
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

def should_drop_by_title(title):
    if not title or title.strip()=="":
        return True
    if ROLE_WORDS_RE.search((title or "").lower()): return False
    if re.search(r'\bintern\b', (title or "").lower()): return False
    return True
def _iso_only_date(raw):
    if not raw:
        return ""
    raw = raw.strip()
    try:
        return datetime.fromisoformat(raw.replace("Z", "")).date().isoformat()
    except:
        pass
    try:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
    except:
        pass
    return ""

def extract_date_from_html(html_text):
    if not html_text:
        return ""
    m = re.search(r'"datePosted"\s*:\s*"([^"]+)"', html_text)
    if m:
        raw = m.group(1)
        try:
            return datetime.fromisoformat(raw.split("T")[0]).date().isoformat()
        except:
            pass
    m2 = re.search(r'<time[^>]+datetime=["\']([^"\']+)["\']', html_text, re.I)
    if m2:
        raw = m2.group(1)
        try:
            return datetime.fromisoformat(raw.split("T")[0]).date().isoformat()
        except:
            pass
    mm = re.search(r'posted\s+(\d+)\s+days?\s+ago', html_text, re.I)
    if mm:
        try:
            days = int(mm.group(1))
            return (date.today() - timedelta(days=days)).isoformat()
        except:
            pass
    mm2 = re.search(r'(\d{4}-\d{2}-\d{2})', html_text)
    if mm2:
        return mm2.group(1)
    return ""

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

                # --- REPLACED BLOCK START ---
                if company in SPECIAL_EXTRACTORS_DEEP:
                    print(f"[DEBUG] Running special extractor for {company}")
                    try:
                        special = SPECIAL_EXTRACTORS_DEEP[company](soup, page, main_url)
                        print(f"[DEBUG] SPECIAL RETURNED FOR {company}: {len(special)} items")
                        for cand in special:
                            print("[DEBUG ITEM]", cand)
                    except Exception as e:
                        print(f"[DEBUG] SPECIAL EXTRACTOR ERROR for {company} ->", e)
                else:
                    print(f"[DEBUG] Special extractor NOT found for {company}")
                # --- REPLACED BLOCK END ---

                # generic anchors
                for a in soup.find_all("a", href=True):
                    href = a.get("href"); text = a.get_text(" ", strip=True) or ""
                    href_abs = normalize_link(main_url, href)
                    if is_likely_job_anchor(href_abs, text):
                        candidates.append((href_abs, text, a))

                # preserved Fivetran extractor
                if "fivetran.com" in main_url:
                    for job in soup.select("div[data-job-id], div.job-card, a[data-job-id]"):
                        text = job.get_text(" ", strip=True)
                        link = job.get("href") or job.get("data-url") or job.get("data-job-url") or job.get("data-job-id")
                        if link and not link.startswith("http") and job.get("data-job-id"):
                            link = f"https://www.fivetran.com/careers/job/{job.get('data-job-id')}"
                        link = normalize_link(main_url, link)
                        if text.strip(): candidates.append((link, text, job))

                # job-card containers
                for el in soup.select("[data-job], .job, .job-listing, .job-card, .opening, .position, .posting, .role, .job-row"):
                    a = el.find("a", href=True)
                    text = a.get_text(" ", strip=True) if a else el.get_text(" ", strip=True)
                    href = normalize_link(main_url, a.get("href")) if a else ""
                    if is_likely_job_anchor(href, text):
                        candidates.append((href, text, el))

                # iframe -> ATS fallback
                if not candidates:
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe.get("src")
                        if src and any(k in src for k in ("greenhouse","lever","myworkday","bamboohr","ashby","jobs.lever","jobvite")):
                            src_full = normalize_link(main_url, src)
                            iframe_html = fetch_page_content(page, src_full)
                            if iframe_html:
                                f_soup = BeautifulSoup(iframe_html, "lxml")
                                for a in f_soup.find_all("a", href=True):
                                    href = a.get("href"); text = a.get_text(" ", strip=True) or ""
                                    href_abs = normalize_link(src_full, href)
                                    if is_likely_job_anchor(href_abs, text):
                                        candidates.append((href_abs, text, a))

                # dedupe + company skip rules
                seen = set(); filtered = []
                for href, text, el in candidates:
                    if not href or href.rstrip("/") == main_url.rstrip("/"): continue
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
                    title_candidate = re.sub(r'\s+', ' ', (anchor_text or "")).strip()
                    title_clean, location_candidate = extract_location_from_text(title_candidate)
                    title_clean = clean_title(title_clean or title_candidate)
                    card_loc = try_extract_location_from_card(el)
                    if card_loc and not location_candidate:
                        location_candidate = card_loc

                    light_score = score_title_desc(title_candidate, "", company)
                    posting_date = ""
                    must_detail = False
                    must_reasons = []
                    if company.lower() in CRITICAL_COMPANIES: must_detail = True; must_reasons.append("critical_company")
                    if not location_candidate: must_detail = True; must_reasons.append("no_location")
                    if len((title_clean or "").split()) < 2: must_detail = True; must_reasons.append("short_title")
                    if LOC_RE.search(title_candidate): must_detail = True; must_reasons.append("title_contains_loc")
                    if any(x in (link or "").lower() for x in ["/job/","/jobs/","greenhouse","lever.co","ashby","bamboohr","myworkdayjobs","gr8people","welcometothejungle","jobvite","smartrecruiters"]):
                        must_detail = True; must_reasons.append("link_looks_like_ats")

                    if must_detail:
                        print(f"[DETAIL_DECISION] (forced) {company} {link} reasons={','.join(must_reasons)}")
                    else:
                        print(f"[LIGHT_SCORE] {company} {link} title='{title_candidate[:60]}' score={light_score}")

                    detail_html = ""
                    final_score = light_score

                    if light_score >= RELEVANCY_THRESHOLD and not must_detail:
                        print(f"[KEEP-LIGHT] {company} | {title_candidate} | score={light_score}")
                    else:
                        if light_score <= 0 and not must_detail:
                            print(f"[DROP-LIGHT] Dropping {company} | {title_candidate} score={light_score}")
                            continue
                        if detail_count < MAX_DETAIL_PAGES:
                            detail_count += 1
                            detail_html = fetch_page_content(page, link)
                            if detail_html:
                                try:
                                    s = BeautifulSoup(detail_html, "lxml")
                                    header = s.find("h1")
                                    if header:
                                        newt = clean_title(header.get_text(" ", strip=True))
                                        if newt and newt != title_clean:
                                            title_clean = newt
                                    for sel in ["span.location", ".job-location", ".location", "[data-test='job-location']", ".posting-location", ".job_meta_location", ".location--name"]:
                                        eloc = s.select_one(sel)
                                        if eloc and eloc.get_text(strip=True):
                                            location_candidate = eloc.get_text(" ", strip=True); break
                                    for script in s.find_all("script", type="application/ld+json"):
                                        text = script.string or ""
                                        if not text: continue
                                        try:
                                            payload = json.loads(text)
                                        except:
                                            continue
                                        items = payload if isinstance(payload, list) else [payload]
                                        for item in items:
                                            if isinstance(item, dict):
                                                if isinstance(item.get("datePosted"), str) and not posting_date:
                                                    posting_date = _iso_only_date(item.get("datePosted"))
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
                                                                location_candidate = ", ".join(parts); break
                                    if not posting_date:
                                        posting_date = extract_date_from_html(detail_html)
                                except Exception as e:
                                    print(f"[WARN] detail parse fail {link} -> {e}")

                        final_score = score_title_desc(title_clean or title_candidate, detail_html or "", company)
                        print(f"[FINAL_SCORE] {company} {link} final_score={final_score} (light={light_score})")
                        if final_score < RELEVANCY_THRESHOLD:
                            print(f"[DROP-FINAL] Dropping after detail fetch -> score={final_score} | {company} | title='{title_clean or title_candidate}'")
                            continue

                    title_final = clean_title(title_clean or anchor_text or "")
                    location_candidate = location_candidate or ""
                    posting_date_final = posting_date or ""
                    if not location_candidate and detail_html:
                        m = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', detail_html)
                        if m:
                            city = m.group(1)
                            m2 = re.search(r'"addressCountry"\s*:\s*"([^"]+)"', detail_html)
                            country = m2.group(1) if m2 else ""
                            loc = f"{city}, {country}".strip(", ")
                            if loc: location_candidate = loc
                        if not location_candidate:
                            mm = re.search(r'"additionalLocations"\s*:\s*\[(.+?)\]', detail_html, re.S)
                            if mm:
                                locs_raw = mm.group(1); locs = re.findall(r'\"([^"]+)\"', locs_raw)
                                if locs: location_candidate = locs[0]
                    if not location_candidate:
                        mloc = re.search(r'/(remote|new[-_]york|london|berlin|singapore|bengaluru)[/\-]?', link or "", re.I)
                        if mloc: location_candidate = mloc.group(1).replace('-', ' ').title()
                    if not posting_date_final:
                        posted_from_anchor = re.search(r'posted\s+(\d+)\s+days?\s+ago', anchor_text or "", re.I)
                        if posted_from_anchor:
                            d = date.today() - timedelta(days=int(posted_from_anchor.group(1)))
                            posting_date_final = d.isoformat()

                    if not should_drop_by_title(title_final):
                        rows.append({
                            "Company": company,
                            "Job Title": title_final,
                            "Job Link": link,
                            "Location": (location_candidate or "").strip(),
                            "Posting Date": posting_date_final,
                            "Days Since Posted": ""
                        })
                    else:
                        print(f"[DROP] Dropping non-job row -> {company} | {title_final} | {link}")

        browser.close()
        return rows

# --- MAIN ---
if __name__ == "__main__":
    try:
        all_rows = scrape()
        for r in all_rows:
            r["Seniority"] = detect_seniority(r.get("Job Title",""))
        dedup = {}
        for r in all_rows:
            lk = r.get("Job Link") or ""
            if lk in dedup:
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
        repo_root = os.path.dirname(os.path.abspath(__file__))
        outfile = os.path.join(repo_root, "jobs_final_hard.csv")
        fieldnames=["Company","Job Title","Job Link","Location","Posting Date","Days Since Posted","Seniority"]
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in out_sorted:
                row_to_write = {k: (r.get(k,"") if r.get(k) is not None else "") for k in fieldnames}
                writer.writerow(row_to_write)
        print(f"[OK] wrote {len(out_sorted)} rows -> {outfile}")
    except KeyboardInterrupt:
        print("Interrupted"); sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}"); sys.exit(1)
