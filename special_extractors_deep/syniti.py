# extractor_syniti.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(syniti|data|migration|governance|integration|etl|engineer)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(1000)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc_el = s.select_one(".job-location, .location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        return title, loc, ""
    except:
        return "", "", ""

def extract_syniti(soup, page, base_url):
    out = []

    for row in soup.select("a[href*='/job/']"):
        link = urljoin(base_url, row.get("href"))
        txt = row.get_text(" ", strip=True)

        if not RELEVANT.search(txt): continue

        title, loc, _ = fetch_detail(page, link)
        if RELEVANT.search(title):
            out.append((link, title, None))

    return out
