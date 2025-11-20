# exasol.py
# Deep extractor for Exasol (static structured HTML jobs list)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_exasol(soup, page, base_url):
    results = []
    seen = set()

    # Exasol job items have predictable containers:
    #   <div class="jobs-list-item"> or <li class="job"> etc.
    card_selectors = [
        ".jobs-list-item a",     # primary structure
        ".job a",                # fallback
        "a[href*='/en/jobs']"    # worst-case static fallback
    ]

    for sel in card_selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            text = a.get_text(" ", strip=True)

            if not href or not text:
                continue

            link = urljoin(base_url, href)

            # avoid duplicates
            if link in seen:
                continue
            seen.add(link)

            # location extraction from parent container
            parent = a.parent
            loc = ""

            if parent:
                loc_el = parent.select_one(".location")
                if loc_el:
                    loc = loc_el.get_text(" ", strip=True)

            label = f"{text} ({loc})" if loc else text

            results.append((link, label))

    return results
