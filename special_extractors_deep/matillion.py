# special_extractors_deep/matillion.py — v1.0
# Matillion uses Lever — use the public API

import requests
from bs4 import BeautifulSoup

def extract_matillion(soup, page, main_url):
    API_URL = "https://api.lever.co/v0/postings/matillion?mode=json"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        jobs = r.json()
    except Exception as e:
        print(f"[Matillion Lever API error] {e}")
        return _dom_fallback(main_url)

    for job in jobs:
        title = (job.get("text") or "").strip()
        link  = (job.get("hostedUrl") or job.get("applyUrl") or "").strip()
        if not title or not link:
            continue

        loc = job.get("categories", {}).get("location") or job.get("country") or ""
        team = job.get("categories", {}).get("team") or ""
        commitment = job.get("categories", {}).get("commitment") or ""

        posting_date = ""
        ts = job.get("createdAt")
        if ts:
            try:
                from datetime import datetime
                posting_date = datetime.fromtimestamp(ts / 1000).date().isoformat()
            except Exception:
                pass

        # Description from Lever
        desc = ""
        desc_html = (job.get("descriptionPlain") or
                     job.get("description") or
                     job.get("additionalPlain") or "")
        if desc_html:
            try:
                desc = BeautifulSoup(desc_html, "lxml").get_text(" ", strip=True)[:4000]
            except Exception:
                desc = desc_html[:4000]

        out.append((link, title, desc, str(loc), posting_date))

    print(f"[Matillion Lever API] Extracted {len(out)} jobs")
    return out


def _dom_fallback(base_url):
    """Lever DOM is predictable — /job/ links."""
    return []  # will fall through to generic pipeline
