# special_extractors_deep/extract_vertica.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_vertica(soup, page, main_url):
    """
    Deep extractor for Vertica (OpenText / Workday style)
    URL:
        https://careers.opentext.com/us/en/home

    Logic:
        - Detect Workday/OpenText job cards
        - Extract link + title
        - Provide stable match for your pipeline
    """

    results = []

    # ------------------------------------------------------
    # 1) Standard Workday job links (OpenText uses these)
    # ------------------------------------------------------
    for a in soup.select(
        "a.jobTitle-link, "
        "a[data-automation-id='jobTitle'], "
        "a.WD-Text"
    ):
        href = a.get("href")
        if not href:
            continue
        link = urljoin(main_url, href)
        title = a.get_text(" ", strip=True)
        if title:
            results.append((link, title, a))

    if results:
        return results

    # ------------------------------------------------------
    # 2) OpenText-specific job card container fallback
    # ------------------------------------------------------
    # Example:
    # <div class="job-result"> <a href="/job/...">Data Engineer</a> </div>
    for card in soup.select("div.job-result, div.job, div.job-list-item"):
        a = card.find("a", href=True)
        if not a:
            continue

        link = urljoin(main_url, a["href"])
        title = a.get_text(" ", strip=True)
        if title:
            results.append((link, title, a))

    if results:
        return results

    # ------------------------------------------------------
    # 3) Generic Workday fallback: find any /job/ links
    # ------------------------------------------------------
    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_l = href.lower()

        if (
            "/job/" in href_l
            or "workday" in href_l
            or "wd" in href_l
            or "opentext" in href_l
        ):
            title = a.get_text(" ", strip=True)
            if not title:
                continue

            link = urljoin(main_url, href)
            results.append((link, title, a))

    return results
