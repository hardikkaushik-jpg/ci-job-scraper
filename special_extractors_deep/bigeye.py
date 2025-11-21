from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_bigeye(soup, page, main_url):
    """
    Returns list of (link, title) OR (link, title, el).
    Gem boards require JS → we load via page (already loaded by main script).
    """
    out = []

    # 1. Try direct anchor scan
    anchors = soup.find_all("a", href=True)
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if "/bigeye" in href or "gem.com" in href:
            text = a.get_text(" ", strip=True)
            if text and len(text) > 2:
                out.append((href, text, a))

    # 2. If nothing found → load the true Gem iframe URL
    # (your current main_url for BigEye is WRONG: https://www.bigeye.com/careers#positions
    #   → no jobs are actually inside this HTML, they are inside Gem.)
    if not out:
        gem_url = "https://jobs.gem.com/bigeye?embed=true"
        try:
            page.goto(gem_url, wait_until="networkidle", timeout=45000)
            page.wait_for_timeout(2500)
            html = page.content()
            s2 = BeautifulSoup(html, "lxml")
            for a in s2.find_all("a", href=True):
                href = a.get("href")
                if href and "/bigeye" in href:
                    text = a.get_text(" ", strip=True)
                    if text:
                        out.append((urljoin(gem_url, href), text))
        except Exception:
            pass

    return out
