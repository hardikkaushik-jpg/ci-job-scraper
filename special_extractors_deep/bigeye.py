# bigeye.py — FINAL WORKING VERSION for jobs.gem.com
# Gem job boards require JS hydration before DOM appears.

from bs4 import BeautifulSoup
from urllib.parse import urljoin

GEM_URL = "https://jobs.gem.com/bigeye"

def extract_bigeye(soup, page, base_url):
    results = []
    seen = set()

    # 1) Load real Gem job board
    try:
        page.goto(GEM_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_selector("a[href*='/bigeye/']", timeout=15000)  # CRITICAL
        html = page.inner_html("body")  # Use DOM, NOT page.content()
    except Exception as e:
        print("[BIGEYE-EXTRACTOR] Failed:", e)
        return results

    s = BeautifulSoup(html, "lxml")

    # 2) Extract hydrated job cards
    cards = s.select("a[href*='/bigeye/']")
    for a in cards:
        href = a.get("href", "").strip()
        if not href:
            continue

        # Normalize link
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
