# extractor_sifflet.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(observab|data|quality|engineer|analytics)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=33000, wait_until="networkidle")
        page.wait_for_timeout(700)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = s.select_one("span[itemprop='addressLocality'], .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        return title, loc, ""
    except:
        return "", "", ""

def extract_sifflet(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/jobs/']"):
        href = urljoin(base_url, a.get("href"))
        txt = a.get_text(" ", strip=True)

        if not RELEVANT.search(txt): continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
