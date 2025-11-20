# qlik.py
# Deep extractor for Qlik CareerHub

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_qlik(soup, page, base_url):
    results = []
    seen = set()

    # Qlik loads jobs dynamically — force JS rendering
    try:
        page.goto(base_url, wait_until="networkidle", timeout=40000)
        page.wait_for_load_state("networkidle")
        time.sleep(1.2)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    job_cards = []

    selectors = [
        "a.career-site-job",
        "div.search-results a",
        "div.job a",
        "a[href*='job']",
    ]

    for sel in selectors:
        job_cards.extend(soup.select(sel))

    for a in job_cards:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # Title extraction
        title = a.get_text(" ", strip=True)

        # Fallback to parent (Cornerstone sometimes wraps text outside anchor)
        if not title or len(title) < 3:
            parent = a.find_parent()
            if parent:
                title = parent.get_text(" ", strip=True)

        if not title or len(title) < 3:
            continue

        # Location extraction
        loc = ""
        parent = a.find_parent()
        possible = []

        if parent:
            for sel in [".location", ".job-location", ".posting-location", ".meta", ".details"]:
                el = parent.select_one(sel)
                if el and el.get_text(strip=True):
                    possible.append(el.get_text(" ", strip=True))

        if possible:
            loc = possible[0]

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
