# extractor_yellowbrick.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(yellowbrick|data|warehouse|sql|analytics|bi|engineer)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(800)

        s = BeautifulSoup(page.content(), "lxml")

        h = s.find("h1")
        title = h.get_text(" ", strip=True) if h else ""

        loc = ""
        loc_el = s.select_one(".location, .job-location")
        if loc_el: loc = loc_el.get_text(" ", strip=True)

        return title, loc, ""
    except:
        return "", "", ""

def extract_yellowbrick(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/careers/']"):
        href = urljoin(base_url, a.get("href"))
        t = a.get_text(" ", strip=True)

        if not RELEVANT.search(t): continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
