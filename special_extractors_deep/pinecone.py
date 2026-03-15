# special_extractors_deep/pinecone.py — v1.0
# Pinecone uses Ashby — API first, DOM fallback

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

ASHBY_URL = "https://jobs.ashbyhq.com/pinecone"

def extract_pinecone(soup, page, main_url):
    API_URL = "https://api.ashbyhq.com/posting-api/job-board/pinecone"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Pinecone Ashby API error] {e}")
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

    print(f"[Pinecone Ashby API] Extracted {len(out)} jobs")
    return out


def _dom_fallback(page):
    out = []
    seen = set()
    try:
        page.goto(ASHBY_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1200)
        s = BeautifulSoup(page.content(), "lxml")
        import re
        for a in s.select("a[href*='pinecone'], a[href*='/job/']"):
            href = a.get("href", "").strip()
            if not href or not re.search(r"[0-9a-fA-F\-]{8,}", href):
                continue
            link = href if href.startswith("http") else urljoin(ASHBY_URL, href)
            if link in seen:
                continue
            seen.add(link)
            title = a.get_text(" ", strip=True)
            if title and len(title.split()) >= 2:
                out.append((link, title, "", "", ""))
    except Exception as e:
        print(f"[Pinecone DOM fallback error] {e}")
    return out
