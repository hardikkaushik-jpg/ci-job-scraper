from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_bigeye(soup, page, main_url):
    """
    Ignore main_url completely.
    Always load BigEye Gem job board.
    """
    GEM_URL = "https://jobs.gem.com/bigeye?embed=true"

    out = []

    try:
        page.goto(GEM_URL, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(2000)
        html = page.content()
        s = BeautifulSoup(html, "lxml")

        for a in s.find_all("a", href=True):
            href = a.get("href")
            if href and "/bigeye/" in href:
                text = a.get_text(" ", strip=True)
                if text and len(text) >= 2:
                    out.append((urljoin(GEM_URL, href), text))
    except Exception as e:
        print("[BigEye extractor error]", e)

    return out
