# informatica.py — FINAL CLEAN VERSION

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GR8_URL = "https://informatica.gr8people.com/jobs"

def extract_informatica(soup, page, main_url):
    """
    Informatica deep extractor
    Always bypass marketing site and load GR8People job board.
    Returns clean (link, title, element) tuples.
    """

    results = []

    # 1) Always fetch real job board
    try:
        page.goto(GR8_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # 2) GR8People job cards
    cards = s.select("section.search-results article, div.search-result")

    for c in cards:
        a = c.find("a", href=True)
        if not a:
            continue

        link = urljoin(GR8_URL, a["href"])
        title = a.get_text(" ", strip=True)

        # skip garbage rows
        if not title or len(title) < 3:
            continue

        results.append((link, title, c))

    return results
