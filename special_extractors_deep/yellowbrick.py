# special_extractors_deep/extract_yellowbrick.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_yellowbrick(soup, page, main_url):
    """
    Deep extractor for Yellowbrick careers.
    URL:
        https://yellowbrick.com/company/careers/

    Yellowbrick loads jobs into <a> tags inside job cards.
    Often links go to Greenhouse or Jobvite.
    """

    results = []

    # ------------------------------------------------------
    # 1) Direct job cards
    # ------------------------------------------------------
    # Typical structure:
    # <a class="job-card" href="https://boards.greenhouse.io/...">
    for a in soup.select(
        "a.job-card, a.job, a.career-job, div.job-card a, div.job a"
    ):
        href = a.get("href")
        if not href:
            continue

        title = a.get_text(" ", strip=True)
        if not title:
            continue

        link = urljoin(main_url, href)
        results.append((link, title, a))

    if results:
        return results

    # ------------------------------------------------------
    # 2) Generic fallback: catch ATS outbound links
    # ------------------------------------------------------
    for a in soup.find_all("a", href=True):
        href = a["href"]
        h = href.lower()

        # Yellowbrick uses ATS platforms
        if (
            "greenhouse" in h
            or "jobvite" in h
            or "/jobs/" in h
            or "/job/" in h
        ):
            title = a.get_text(" ", strip=True)
            if not title:
                continue

            link = urljoin(main_url, href)
            results.append((link, title, a))

    return results
