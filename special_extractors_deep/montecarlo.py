# special_extractors_deep/montecarlo.py — v2.0
# Monte Carlo uses Ashby — hit the API directly

import requests

def extract_montecarlo(soup, page, main_url):
    # Ashby public job board API
    API_URL = "https://api.ashbyhq.com/posting-api/job-board/montecarlodata"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[MonteCarlo Ashby API error] {e}")
        # Fallback to DOM
        return _dom_fallback(soup, main_url)

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

        desc_text = (job.get("descriptionHtml") or job.get("description") or "")[:4000]

        out.append((link, title, desc_text, str(loc), posting_date))

    print(f"[MonteCarlo API] Extracted {len(out)} jobs")
    return out


def _dom_fallback(soup, base_url):
    """DOM fallback if API fails."""
    from urllib.parse import urljoin
    out = []
    seen = set()
    for a in soup.select("a[href*='/job/'], a[href*='montecarlodata']"):
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
