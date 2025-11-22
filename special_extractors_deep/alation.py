# special_extractors_deep/alation.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_alation(soup, page, base_url):
    results = []
    seen = set()

    # The jobs are inside: div.job-result or div.career-item
    job_cards = soup.select("div.job-result, div.career-item, li.careers-listing__item")

    if not job_cards:
        print("[ALATION] No job cards found on page")
        return results

    for card in job_cards:
        # Title
        title_el = card.select_one("a, h3, .careers-listing__title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)

        # Link
        href = title_el.get("href", "").strip()
        if not href:
            continue
        link = urljoin(base_url, href)

        # Location
        loc_el = card.select_one(".location, .careers-listing__location, .job-location")
        location = loc_el.get_text(strip=True) if loc_el else ""

        key = (link, title)
        if key in seen:
            continue
        seen.add(key)

        results.append((link, title, location))

    return results
