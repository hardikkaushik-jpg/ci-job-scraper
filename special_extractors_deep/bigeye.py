# bigeye.py
# Deep extractor for BigEye (static HTML job cards)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_bigeye(soup, page, base_url):
    results = []

    # BigEye renders job cards inside <div class="careers-list"> or <div class="positions">
    # Each card typically contains:
    #   <a href="/careers/<slug>">Job Title</a>
    #   <div class="location">...</div>

    job_selectors = [
        ".positions a",               # older site layout
        ".careers-list a",            # current layout
        ".job-card a",                # fallback
        "a[href*='/careers']"         # worst-case fallback
    ]

    seen = set()

    for sel in job_selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            text = a.get_text(" ", strip=True)

            if not href or not text:
                continue

            # Normalize URL
            link = urljoin(base_url, href)

            # Dedupe
            if link in seen:
                continue
            seen.add(link)

            # Try to detect location from nearby elements
            parent = a.parent
            loc = ""

            # Check sibling/parent
            if parent:
                loc_el = parent.select_one(".location")
                if loc_el:
                    loc = loc_el.get_text(" ", strip=True)

            # Put location into the combined label
            label = f"{text} ({loc})" if loc else text

            results.append((link, label))

    return results
