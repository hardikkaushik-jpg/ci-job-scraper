# collibra.py — FINAL WORKING, GREENHOUSE-STABLE VERSION

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GREENHOUSE_URL = "https://boards.greenhouse.io/collibra"

def extract_collibra(soup, page, base_url):
    results = []
    seen = set()

    try:
        # 1) Load Greenhouse board
        page.goto(GREENHOUSE_URL, timeout=45000, wait_until="networkidle")

        # 2) Wait for job sections to hydrate
        page.wait_for_selector("section.openings-group, div.opening, a[href*='/jobs/']",
                               timeout=15000)

        # 3) Extract the ACTUAL DOM (NOT page.content())
        html = page.inner_html("body")

    except Exception as e:
        print("[COLLIBRA-EXTRACTOR] Failure:", e)
        return results

    gh = BeautifulSoup(html, "lxml")

    # 4) Greenhouse uses <section class='opening'> or grouped listings
    selectors = [
        "div.opening > a",
        "section.opening a",
        "section.openings-group a[href*='/jobs/']",
        "a[href*='/collibra/']",
        "a[href*='/jobs/']"
    ]

    links = []
    for sel in selectors:
        links.extend(gh.select(sel))

    for a in links:
        href = a.get("href", "").strip()
        if not href or "/jobs/" not in href:
            continue

        # Normalize
        link = urljoin(GREENHOUSE_URL, href)

        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)
        if not title or len(title) < 2:
            continue

        results.append((link, title))

    return results
