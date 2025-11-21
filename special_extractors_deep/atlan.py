# atlan.py — FIXED VERSION
# Deep extractor for Atlan (AshbyHQ platform)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_atlan(soup, page, base_url):
    results = []
    ashby_url = "https://jobs.ashbyhq.com/atlan"
    seen = set()

    try:
        # Load Ashby board
        page.goto(ashby_url, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # Ashby job cards:
    cards = s.select(
        "a[href*='/atlan/'], a[href*='/job/'], a[href*='ashbyhq.com/atlan/']"
    )

    for a in cards:
        href = a.get("href", "").strip()
        if not href:
            continue

        # Normalize link
        if href.startswith("/"):
            link = "https://jobs.ashbyhq.com" + href
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        # Extract title
        title = a.get_text(" ", strip=True)
        if not title:
            continue

        # Filter garbage
        if len(title) < 3:
            continue
        if "Ashby" in title.lower():
            continue

        results.append((link, title))

    return results
