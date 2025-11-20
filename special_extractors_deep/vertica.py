# extractor_vertica.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(vertica|analytics|warehouse|sql|columnar|engineer|data)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=38000, wait_until="networkidle")
        page.wait_for_timeout(1000)

        s = BeautifulSoup(page.content(), "lxml")

        t = s.find("h1")
        title = t.get_text(" ", strip=True) if t else ""

        loc_el = s.select_one(".job-location, .location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        return title, loc, ""
    except:
        return "", "", ""

def extract_vertica(soup, page, base_url):
    out = []

    for card in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, card.get("href"))
        text = card.get_text(" ", strip=True)

        if not RELEVANT.search(text): continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
