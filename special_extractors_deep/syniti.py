# special_extractors_deep/extract_syniti.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_syniti(soup, page, main_url):
    """
    Deep extractor for Syniti (Workday ATS)
    Example URL:
        https://careers.syniti.com/go/Explore-Our-Roles/8777900/

    Logic:
        - Detect Workday job cards
        - Extract title + job link
        - Return exact HTML element for scoring
    """

    results = []

    # ---------------------------------------------
    # 1) Standard Workday job cards
    # ---------------------------------------------
    # Most Syniti jobs appear in:
    #   <a class="jobTitle-link" href="/en-US/.../job/...">
    #
    for a in soup.select("a.jobTitle-link, a.WD-Text, a[data-automation-id='jobTitle']"):
        href = a.get("href")
        if not href:
            continue

        link = urljoin(main_url, href)
        title = a.get_text(" ", strip=True)
        if not title:
            continue

        results.append((link, title, a))

    if results:
        return results

    # ---------------------------------------------
    # 2) Workday table fallback format
    # ---------------------------------------------
    rows = soup.select("tr[data-automation-id='job'] a[href]")
    for a in rows:
        link = urljoin(main_url, a["href"])
        title = a.get_text(" ", strip=True)
        if title:
            results.append((link, title, a))

    if results:
        return results

    # ---------------------------------------------
    # 3) Last fallback: any anchor containing /job/
    # ---------------------------------------------
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        if "job" in h and ("syniti" in h or "wd" in h or "workday" in h):
            link = urljoin(main_url, a["href"])
            title = a.get_text(" ", strip=True)
            if title:
                results.append((link, title, a))

    return results
