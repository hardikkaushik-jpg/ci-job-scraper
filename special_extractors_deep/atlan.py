# special_extractors_deep/atlan.py — v2.0
# Atlan uses Ashby — API first, DOM fallback

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

ASHBY_URL = "https://jobs.ashbyhq.com/atlan"

def extract_atlan(soup, page, main_url):
    API_URL = "https://api.ashbyhq.com/posting-api/job-board/atlan"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Atlan Ashby API error] {e}")
        return _dom_fallback(page)

    for job in data.get("jobs", []):
        title = (job.get("title") or "").strip()
        link  = (job.get("jobUrl") or "").strip()
        if not title or not link:
            continue

        loc = job.get("locationName") or job.get("location") or ""
        if isinstance(loc, dict):
            loc = loc.get("name", "")

        posting_date = ""
        pub = job.get("publishedDate") or job.get("createdAt") or ""
        if pub:
            posting_date = pub.split("T")[0]

        desc = (job.get("descriptionHtml") or job.get("description") or "")
        if desc:
            try:
                desc = BeautifulSoup(desc, "lxml").get_text(" ", strip=True)[:4000]
            except Exception:
                desc = desc[:4000]

        out.append((link, title, desc, str(loc), posting_date))

    print(f"[Atlan API] Extracted {len(out)} jobs")
    return out


def _dom_fallback(page):
    out = []
    seen = set()
    try:
        page.goto(ASHBY_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1000)
        s = BeautifulSoup(page.content(), "lxml")
        for a in s.select("a[href*='/atlan/'], a[href*='/job/'], a[href*='ashbyhq.com/atlan/']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            link = ("https://jobs.ashbyhq.com" + href
                    if href.startswith("/") else href)
            if link in seen:
                continue
            seen.add(link)
            title = a.get_text(" ", strip=True)
            if title and len(title) >= 3 and "ashby" not in title.lower():
                out.append((link, title, "", "", ""))
    except Exception as e:
        print(f"[Atlan DOM fallback error] {e}")
    return out
