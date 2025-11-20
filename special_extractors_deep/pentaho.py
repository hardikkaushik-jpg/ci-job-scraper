# special_extractors_deep/extract_pentaho.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_pentaho(soup, page, main_url):
    """
    Deep extractor for Pentaho (Hitachi Vantara)
    Covers:
        - https://www.hitachivantara.com/en-us/company/careers/job-search
        - https://www.hitachivantara.com/en-us/careers.html (iframe loader)
    Returns list of: (link, title, element)
    """

    results = []

    # ---------------------------------------------
    # CASE 1: Direct job-search page (Hitachi ATS)
    # ---------------------------------------------
    if "job-search" in main_url or "hitachivantara" in main_url:
        # job cards appear as <a class="search-result-card"> or similar
        cards = soup.select("a.search-result-card, a.career-result, div.search-result a")
        for c in cards:
            link = c.get("href")
            if not link:
                continue

            link = urljoin(main_url, link)
            title = c.get_text(" ", strip=True)
            if not title:
                continue

            results.append((link, title, c))

        if results:
            return results

    # -------------------------------------------------------
    # CASE 2: Marketing page → iframe → jobs feed
    # -------------------------------------------------------
    iframe = soup.find("iframe", src=True)
    if iframe and "hitachivantara" in iframe.get("src", "").lower():
        iframe_url = urljoin(main_url, iframe["src"])
        try:
            page.goto(iframe_url, timeout=35000, wait_until="networkidle")
            page.wait_for_timeout(700)
            ih = page.content()
        except:
            return results

        i_soup = BeautifulSoup(ih, "lxml")
        cards = i_soup.select("a.search-result-card, a.career-result, div.search-result a")

        for c in cards:
            href = c.get("href")
            if not href:
                continue

            link = urljoin(iframe_url, href)
            title = c.get_text(" ", strip=True)
            if not title:
                continue

            results.append((link, title, c))

        return results

    # ----------------------------------------------------------------
    # Fallback: Any anchor pointing to Hitachi Vantara job detail page
    # ----------------------------------------------------------------
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        if "hitachivantara" in h and ("job" in h or "careers" in h):
            link = urljoin(main_url, a["href"])
            title = a.get_text(" ", strip=True)
            if title:
                results.append((link, title, a))

    return results
