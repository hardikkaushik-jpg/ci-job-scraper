from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    """
    Ignore Collibra marketing URL.
    Always load official Greenhouse board.
    """
    GH_URL = "https://boards.greenhouse.io/collibra"

    out = []

    try:
        page.goto(GH_URL, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(2000)
        html = page.content()
        s = BeautifulSoup(html, "lxml")

        for a in s.find_all("a", href=True):
            href = a.get("href")
            if href and "/jobs/" in href:
                text = a.get_text(" ", strip=True)
                if text and len(text) >= 2:
                    out.append((urljoin(GH_URL, href), text))
    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
