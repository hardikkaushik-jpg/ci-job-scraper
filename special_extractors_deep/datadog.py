# special_extractors_deep/datadog.py — v2.0
# Datadog uses Greenhouse — use the API directly

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_datadog(soup, page, base_url):
    API_URL = "https://boards-api.greenhouse.io/v1/boards/datadoghq/jobs?content=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Datadog Greenhouse API error] {e}")
        return _dom_fallback(soup, base_url)

    for job in data.get("jobs", []):
        title = (job.get("title") or "").strip()
        link  = (job.get("absolute_url") or "").strip()
        if not title or not link:
            continue

        loc = ""
        loc_data = job.get("location")
        if isinstance(loc_data, dict):
            loc = loc_data.get("name", "")

        posting_date = ""
        for df in ("first_published_at", "updated_at"):
            raw = job.get(df, "")
            if raw:
                posting_date = raw.split("T")[0]
                break

        desc_text = ""
        desc_html = job.get("content", "")
        if desc_html:
            try:
                desc_text = BeautifulSoup(desc_html, "lxml").get_text(" ", strip=True)[:4000]
            except Exception:
                pass

        out.append((link, title, desc_text, loc, posting_date))

    print(f"[Datadog API] Extracted {len(out)} jobs")
    return out


def _dom_fallback(soup, base_url):
    """DOM fallback in case API is unavailable."""
    out = []
    seen = set()
    for a in soup.select("a[href*='/job/']"):
        href = a.get("href", "").strip()
        if not href:
            continue
        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)
        title = a.get_text(" ", strip=True)
        if title:
            out.append((link, title, "", "", ""))
    return out
