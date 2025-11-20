# extractor_teradata.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(teradata|sql|analytics|warehouse|data|pipeline|etl|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=42000, wait_until="networkidle")
        page.wait_for_timeout(1100)

        s = BeautifulSoup(page.content(), "lxml")

        h = s.find("h1")
        title = h.get_text(" ", strip=True) if h else ""

        loc = ""
        loc_el = s.select_one(".location, .job-location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        return title, loc, dt

    except:
        return "", "", ""

def extract_teradata(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text): continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
