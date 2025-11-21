# ataccama.py
# Correct extractor for Ataccama (jobs.ataccama.com)

from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

ATACCAMA_URL = "https://jobs.ataccama.com/"

def extract_ataccama(soup, page, base_url):
    results = []
    seen = set()

    # Load the real careers page
    try:
        page.goto(ATACCAMA_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(800)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # Job cards use href with UUID
    cards = s.select("a[href*='ataccama.com/']")
    for a in cards:
        href = a.get("href")
        if not href:
            continue

        link = href if href.startswith("http") else urljoin(ATACCAMA_URL, href)

        # Valid UUID-format job pages
        if not re.search(r"[0-9a-fA-F\-]{36}", link):
            continue

        if link in seen:
            continue
        seen.add(link)

        # Extract title from text or <h3>
        title = a.get_text(" ", strip=True)
        if not title:
            h3 = a.find("h3")
            if h3:
                title = h3.get_text(" ", strip=True)

        if not title:
            continue

        # Ignore junk
        if len(title.split()) < 2:
            continue

        results.append((link, title))

    return results
