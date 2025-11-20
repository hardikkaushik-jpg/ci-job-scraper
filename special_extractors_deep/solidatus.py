# solidatus.py
# Deep extractor for Solidatus careers page

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_solidatus(soup, page, base_url):
    results = []
    seen = set()

    # Solidatus uses simple job cards with anchors linking to Ashby or /careers/*
    selectors = [
        "a[href*='ashby']",
        "a[href*='/careers/']",
        "div.vacancy a",
        "div.job a",
        "div.opening a",
        "div.careers__item a"
    ]

    anchors = []
    for sel in selectors:
        anchors.extend(soup.select(sel))

    for a in anchors:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # Title extraction
        title = a.get_text(" ", strip=True)

        # Fallback: use parent text if anchor insufficient
        if not title or len(title) < 3:
            parent = a.find_parent()
            if parent:
                title = parent.get_text(" ", strip=True)

        if not title or len(title) < 3:
            continue

        # Location detection inside card or nearby element
        loc = ""
        possible_locs = []
        parent = a.find_parent()

        if parent:
            for sel in [".location", ".job-location", ".posting-location", ".meta", ".details"]:
                el = parent.select_one(sel)
                if el and el.get_text(strip=True):
                    possible_locs.append(el.get_text(" ", strip=True))

        if possible_locs:
            loc = possible_locs[0]

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
