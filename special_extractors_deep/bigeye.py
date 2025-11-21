# bigeye.py — UPDATED for Gem jobs board
# Deep extractor for BigEye (Gem ATS)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GEM_URL = "https://jobs.gem.com/bigeye"

def extract_bigeye(soup, page, base_url):
    results = []
    seen = set()

    try:
        page.goto(GEM_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # Gem job cards typically use <a href="/bigeye/xxxxx">Title</a>
    for a in s.select("a[href*='/bigeye/']"):
        href = a.get("href", "").strip()
        if not href:
            continue

        if href.startswith("/"):
            link = urljoin(GEM_URL, href)
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)
        if not title or len(title) < 2:
            continue

        results.append((link, title))

    return results
