# anomalo.py
# Deep extractor for Anomalo (Greenhouse ATS)

from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

def extract_anomalo(soup, page, base_url):
    results = []

    # Greenhouse structure:
    # <div class="opening">
    #    <a href="/anomalojobs/...">Job Title</a>
    #    <span class="location">Location</span>
    # </div>

    for opening in soup.select("div.opening"):
        a = opening.find("a", href=True)
        if not a:
            continue

        link = a["href"].strip()
        title = a.get_text(" ", strip=True)

        # Normalize link
        link = urljoin(base_url, link)

        # location
        loc_el = opening.find("span", class_="location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        # Clean title
        title = re.sub(r"\s+", " ", title).strip()

        # Skip garbage
        if not title or len(title) < 2:
            continue

        results.append((link, f"{title} ({loc})" if loc else title))

    return results
