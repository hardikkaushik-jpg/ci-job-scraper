# collibra.py
# Special extractor for Collibra (Greenhouse embedded widget)

from bs4 import BeautifulSoup
import time

def extract_collibra(soup, page, main_url):
    results = []

    # 1) Locate the Greenhouse iframe
    iframe = soup.find("iframe", src=True)
    if not iframe:
        return results

    src = iframe.get("src")
    if not src:
        return results

    # 2) Load Greenhouse iframe URL
    try:
        page.goto(src, timeout=45000, wait_until="networkidle")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)
        gh_html = page.content()
    except Exception:
        return results

    gh = BeautifulSoup(gh_html, "lxml")

    # 3) Extract job cards
    postings = gh.select("div.opening, div.job, div opening, a[href*='/jobs/']")
    if not postings:
        postings = gh.find_all("a", href=True)

    seen = set()

    for p in postings:
        href = p.get("href")
        if not href:
            continue
        if "/jobs/" not in href:
            continue

        # absolute link
        if href.startswith("/"):
            link = "https://boards.greenhouse.io" + href
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        # title extraction
        text = p.get_text(" ", strip=True)
        if not text:
            continue

        results.append((link, text))

    return results
