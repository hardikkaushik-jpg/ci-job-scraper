# anomalo.py
# Deep extractor for Anomalo (AshbyHQ ATS)

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

ANOMALO_URL = "https://jobs.ashbyhq.com/anomalo/"

def extract_anomalo(soup, page, base_url):
    results = []
    seen = set()

    # Load the actual Ashby board
    try:
        page.goto(ANOMALO_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # Ashby job card selector
    cards = s.select("a[href*='anomalo']")
    for a in cards:
        href = a.get("href", "").strip()
        if not href:
            continue

        link = href if href.startswith("http") else urljoin(ANOMALO_URL, href)

        # Must contain an Ashby job UUID
        if not re.search(r"[0-9a-fA-F\-]{36}", link):
            continue

        if link in seen:
            continue
        seen.add(link)

        # Extract title
        title = a.get_text(" ", strip=True)
        if not title:
            continue

        # Skip garbage
        if len(title.split()) < 2:
            continue

        # Try extracting location nearby
        loc = ""
        parent = a.parent
        if parent:
            loc_el = parent.find("div", string=re.compile(r"remote|usa|europe|san|london", re.I))
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)

        label = f"{title} ({loc})" if loc else title
        results.append((link, label))

    return results
