# extractor_bigeye.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(observability|data|pipeline|cloud|integration|etl|warehouse|sql)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(600)
        s = BeautifulSoup(page.content(), "lxml")

        h = s.find("h1")
        title = h.get_text(" ", strip=True) if h else ""

        loc = ""
        loc_el = s.select_one(".job-location, .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        return title, loc, dt
    except:
        return "", "", ""

def extract_bigeye(soup, page, base_url):
    out = []

    positions_section = soup.find(id="positions")
    if not positions_section:
        return out

    for a in positions_section.select("a[href]"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
