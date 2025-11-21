# collibra.py — FINAL WORKING VERSION
# Collibra uses Greenhouse at: https://boards.greenhouse.io/collibra

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GREENHOUSE_URL = "https://boards.greenhouse.io/collibra"

def extract_collibra(soup, page, base_url):
    results = []
    seen = set()

    # Load the real greenhouse board directly
    try:
        page.goto(GREENHOUSE_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    gh = BeautifulSoup(html, "lxml")

    # Standard Greenhouse openings
    openings = gh.select("div.opening > a, a[href*='/collibra/']")
    if not openings:
        openings = gh.select("a[href*='/jobs/']")

    for a in openings:
        href = a.get("href")
        if not href:
            continue

        # Normalize link
        if href.startswith("/"):
            link = "https://boards.greenhouse.io" + href
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)
        if not title:
            continue

        results.append((link, title))

    return results
