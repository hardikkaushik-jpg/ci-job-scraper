# sifflet.py
# Deep extractor for Sifflet (WelcomeToTheJungle)

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_sifflet(soup, page, base_url):
    results = []
    seen = set()

    # Force JS rendering & scroll for lazy load
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)
        for _ in range(3):
            page.mouse.wheel(0, 1200)
            time.sleep(0.8)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    job_cards = []

    selectors = [
        "a[href*='/en/companies/sifflet/jobs/']",
        "article[data-testid='job-card'] a",
        "a[href*='/jobs/']"
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

        # Extract title
        title = a.get_text(" ", strip=True)

        # Try richer extraction from the card
        card = a.find_parent("article")
        location = ""

        if card:
            loc_el = card.select_one("p, span, .sc-eCYdqJ")
            if loc_el:
                location = loc_el.get_text(" ", strip=True)

        # Fallback: use parent containers
        if not location:
            parent = a.find_parent()
            if parent:
                loc_el = parent.find("p")
                if loc_el:
                    location = loc_el.get_text(" ", strip=True)

        label = f"{title} ({location})" if location else title
        results.append((link, label))

    return results
