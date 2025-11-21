# couchbase.py — FINAL WORKING VERSION
# Couchbase uses Greenhouse hosted board: https://boards.greenhouse.io/couchbase

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GH_URL = "https://boards.greenhouse.io/couchbase"

def extract_couchbase(soup, page, base_url):
    results = []
    seen = set()

    # Load the actual Greenhouse job board
    try:
        page.goto(GH_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    gh = BeautifulSoup(html, "lxml")

    # Standard Greenhouse job card pattern
    openings = gh.select("div.opening > a, a[href*='/couchbase/']")
    if not openings:
        openings = gh.select("a[href*='/jobs/']")

    for a in openings:
        href = a.get("href")
        if not href:
            continue

        # Normalize GH link
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

        # Try to extract location (in <span class="location">)
        loc_el = a.find_next("span", class_="location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
