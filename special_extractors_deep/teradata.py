# special_extractors_deep/teradata.py — v2.0
# Workday-based. Returns 5-tuples.
# Removed redundant per-job detail fetch (detail enrichment handled centrally)

import json
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

DOMAIN  = "https://careers.teradata.com"
TENANT  = "teradata"
SITE    = "TeradataCareers"

RELEVANT = re.compile(
    r"\b(data|etl|integration|pipeline|engineer|analyst|architect|"
    r"cloud|platform|bi|analytics|database|sql|developer|sre)\b",
    re.I
)

def extract_teradata(soup, page, base_url):
    out = []
    seen = set()

    # Try Workday cxs API first
    api_root = f"{DOMAIN}/wday/cxs/{TENANT}/{SITE}/jobs"
    offset, limit = 0, 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"
        try:
            r = requests.get(api_url, timeout=20,
                             headers={"Accept": "application/json",
                                      "User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.json()
        except Exception:
            try:
                page.goto(api_url, timeout=30000, wait_until="networkidle")
                raw = page.inner_text("pre") or page.content()
                data = json.loads(raw)
            except Exception as e:
                print(f"[Teradata] API failed at offset {offset}: {e}")
                break

        postings = data.get("jobPostings", [])
        if not postings:
            break

        for job in postings:
            title = (job.get("title") or "").strip()
            path  = job.get("externalPath") or job.get("externalUrl") or ""
            if not title or not path:
                continue

            # Relevance pre-filter
            if not RELEVANT.search(title):
                continue

            link = path if path.startswith("http") else urljoin(base_url, path)
            if link in seen:
                continue
            seen.add(link)

            loc          = (job.get("locationsText") or job.get("location") or "")
            posting_date = (job.get("postedOn") or "").split("T")[0]

            out.append((link, title, "", str(loc), posting_date))

        total = data.get("total", 0)
        offset += limit
        if total and offset >= total:
            break

    # DOM fallback if API returned nothing
    if not out:
        out = _dom_fallback(soup, page, base_url)

    print(f"[Teradata] Extracted {len(out)} jobs")
    return out


def _dom_fallback(soup, page, base_url):
    out = []
    seen = set()
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)
        for _ in range(3):
            page.mouse.wheel(0, 1400)
            time.sleep(0.7)
        soup = BeautifulSoup(page.content(), "lxml")
    except Exception:
        pass

    for a in soup.select("a[href*='/job/'], a[href*='/jobs/']"):
        href = a.get("href", "")
        if not href:
            continue
        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)
        title = a.get_text(" ", strip=True)
        if title and RELEVANT.search(title):
            out.append((link, title, "", "", ""))

    return out
