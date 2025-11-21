# special_extractors_deep/extract_informatica.py

from bs4 import BeautifulSoup
import json, re
from urllib.parse import urljoin

def extract_informatica(soup, page, main_url):
    """
    Deep Informatica extractor.
    Supports:
        - https://informatica.gr8people.com/jobs
        - https://www.informatica.com/us/careers.html (iframe with GR8People)
    Returns: list of (link, title, el)
    """

    results = []

    # -------------------------
    # CASE 1: GR8People direct
    # -------------------------
    if "gr8people.com" in main_url:
        cards = soup.select("section.search-results article, div.search-result")
        for c in cards:
            a = c.find("a", href=True)
            if not a:
                continue

            link = urljoin(main_url, a["href"])
            title = a.get_text(" ", strip=True)
            if not title:
                continue

            results.append((link, title, c))

        return results

    # -------------------------------------------------------
    # CASE 2: Marketing page → iframe containing ATS
    # -------------------------------------------------------
    # Look for iframe with GR8People job listings
    iframe = soup.find("iframe", src=True)
    if iframe and ("gr8people" in iframe["src"].lower()):
        iframe_url = urljoin(main_url, iframe["src"])
        try:
            page.goto(iframe_url, timeout=30000, wait_until="networkidle")
            page.wait_for_timeout(600)
            iframe_html = page.content()
        except:
            return results

        iframe_soup = BeautifulSoup(iframe_html, "lxml")

        cards = iframe_soup.select("section.search-results article, div.search-result")
        for c in cards:
            a = c.find("a", href=True)
            if not a:
                continue
            link = urljoin(iframe_url, a["href"])
            title = a.get_text(" ", strip=True)
            if not title:
                continue
            results.append((link, title, c))

        return results

    # -------------------------------------------------------
    # Fallback: Look for any anchor pointing to Informatica ATS
    # -------------------------------------------------------
    for a in soup.find_all("a", href=True):
        h = a["href"].lower()
        if "gr8people" in h or "informatica" in h:
            link = urljoin(main_url, a["href"])
            title = a.get_text(" ", strip=True)
            if title:
                results.append((link, title, a))

    return results
