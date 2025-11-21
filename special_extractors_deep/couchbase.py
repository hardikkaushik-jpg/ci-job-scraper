# couchbase.py — UPDATED VERSION
# Deep extractor for Couchbase (Greenhouse ATS)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GH_URL = "https://job-boards.greenhouse.io/embed/job_board?for=couchbaseinc"

def extract_couchbase(soup, page, base_url):
    results = []
    seen = set()

    try:
        # Load the Greenhouse embedded job board
        page.goto(GH_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    gh = BeautifulSoup(html, "lxml")

    # Select job links
    openings = gh.select("div.opening > a, a[href*='/couchbaseinc/']")
    if not openings:
        openings = gh.select("a[href*='/jobs/']")

    for a in openings:
        href = a.get("href")
        if not href:
            continue

        if href.startswith("/"):
            link = urljoin("https://boards.greenhouse.io", href)
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)
        if not title:
            continue

        # Extract location if it's nearby
        loc_el = a.find_next("span", class_="location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        label = f"{title} ({loc})" if loc else title
        results.append((link, label))

    return results
