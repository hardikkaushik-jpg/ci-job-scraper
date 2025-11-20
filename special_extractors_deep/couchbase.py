# couchbase.py
# Deep extractor for Couchbase (Greenhouse embedded jobs)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_couchbase(soup, page, base_url):
    results = []
    seen = set()

    # Couchbase career page uses Greenhouse embeds via class `.opening`
    job_rows = soup.select(".opening")

    # Fallback – Look for any anchors pointing to Greenhouse jobs
    if not job_rows:
        job_rows = soup.select("a[href*='greenhouse']")

    for row in job_rows:
        # Extract the anchor
        a = row.find("a", href=True) if row.find("a", href=True) else row
        if not a or not a.get("href"):
            continue

        raw_link = a.get("href").strip()
        link = urljoin(base_url, raw_link)

        if link in seen:
            continue
        seen.add(link)

        # Extract title
        title = a.get_text(" ", strip=True)
        if not title:
            continue

        # Extract location if shown
        loc = ""
        loc_el = row.select_one(".location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        # Combined label for generic extractor contract
        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
