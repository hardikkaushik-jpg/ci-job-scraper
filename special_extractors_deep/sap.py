# extractor_sap.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(sap|hana|analytics|etl|integration|data|engineer|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1200)

        s = BeautifulSoup(page.content(), "lxml")

        h = s.find("h1")
        title = h.get_text(" ", strip=True) if h else ""

        loc_el = s.select_one(".job-location, .location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        return title, loc, ""
    except:
        return "", "", ""

def extract_sap(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text): continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
