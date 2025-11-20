# firebolt.py
# Deep extractor for Firebolt using AshbyHQ job widgets.

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_firebolt(soup, page, base_url):
    results = []
    seen = set()

    # Primary selectors
    job_cards = soup.select(".ashby-job-card")

    # Fallback if site structure changes
    if not job_cards:
        job_cards = soup.select("a[href*='ashby']")

    for card in job_cards:
        # Extract title
        title = ""

        # Standard Ashby title
        t = card.select_one(".ashby-job-card__title")
        if t:
            title = t.get_text(" ", strip=True)

        # Fallback if title was inside the anchor
        if not title:
            a = card.find("a")
            if a:
                title = a.get_text(" ", strip=True)

        if not title:
            continue

        # Extract link
        link = ""
        a = card.select_one("a.ashby-job-card__link") or card.find("a", href=True)
        if a and a.get("href"):
            link = urljoin(base_url, a.get("href").strip())

        if not link:
            continue

        if link in seen:
            continue
        seen.add(link)

        # Extract location
        loc = ""
        l = card.select_one(".ashby-job-card__location")
        if l:
            loc = l.get_text(" ", strip=True)

        # Combine into (link, label)
        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
