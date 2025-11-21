from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_collibra(soup, page, main_url):
    """
    Returns list of (link, title) OR (link, title, el).
    Collibra careers page embeds Greenhouse. We load Greenhouse directly.
    """

    out = []

    # 1. Check if Greenhouse iframe exists
    iframe = soup.find("iframe", src=True)
    gh_url = None
    if iframe:
        src = iframe.get("src")
        if "greenhouse" in src:
            gh_url = urljoin(main_url, src)

    # 2. If iframe missing → fallback to known Collibra board
    if not gh_url:
        gh_url = "https://boards.greenhouse.io/collibra"

    # 3. Load Greenhouse board
    try:
        page.goto(gh_url, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(2500)
        html = page.content()
        s2 = BeautifulSoup(html, "lxml")

        for a in s2.find_all("a", href=True):
            href = a.get("href")
            if href and "/jobs/" in href:
                text = a.get_text(" ", strip=True)
                if text:
                    out.append((urljoin(gh_url, href), text))

    except Exception:
        pass

    return out
