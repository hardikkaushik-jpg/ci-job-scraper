# decube.py
# Deep extractor for Decube (BrioHR ATS)
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_decube(soup, page, base_url):
    results = []
    seen = set()

    # --- Force JS rendering + lazy load scroll ---
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)
        for _ in range(2):
            page.mouse.wheel(0, 1200)
            time.sleep(0.7)

        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    job_cards = []

    selectors = [
        "a[href*='/job/']",
        "[data-testid='job-card'] a",
        "article a[href]",
        "div.card a[href]",
        "a[href*='positions']"
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
        title = a.get_text(" ", strip=True).strip()

        # Clean up empty anchors
        if len(title) < 2:
            continue

        # Location extraction
        location = ""
        card = a.find_parent("article") or a.find_parent("div")

        if card:
            loc_el = (
                card.select_one(".location")
                or card.select_one(".job-location")
                or card.select_one("p")
            )
            if loc_el:
                location = loc_el.get_text(" ", strip=True)

        # Parent-level fallback
        if not location:
            parent = a.find_parent()
            if parent:
                loc_el = parent.find("p")
                if loc_el:
                    location = loc_el.get_text(" ", strip=True)

        label = f"{title} ({location})" if location else title
        results.append((link, label))

    return results
