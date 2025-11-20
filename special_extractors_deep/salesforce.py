# salesforce.py
# Deep extractor for Salesforce careers portal

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_salesforce(soup, page, base_url):
    results = []
    seen = set()

    # Salesforce React site renders job cards with dynamic classes
    job_cards = soup.select("a[href*='/job/'], div[class*='job-card']")

    for card in job_cards:
        # Get anchor
        a = card if card.name == "a" else card.find("a", href=True)
        if not a or not a.get("href"):
            continue

        raw_link = a.get("href").strip()
        if not raw_link:
            continue

        link = urljoin(base_url, raw_link)

        # Dedupe
        if link in seen:
            continue
        seen.add(link)

        # Title
        title = a.get_text(" ", strip=True)

        # If empty, fallback to card content
        if not title:
            title = card.get_text(" ", strip=True)

        if not title or len(title) < 3:
            continue

        # Location try selectors
        loc = ""
        for sel in [".location", ".info", ".job-location", "[data-geography]"]:
            el = card.select_one(sel)
            if el and el.get_text(strip=True):
                loc = el.get_text(" ", strip=True)
                break

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
