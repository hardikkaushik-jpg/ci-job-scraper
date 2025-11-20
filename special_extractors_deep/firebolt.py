# extractor_firebolt.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|warehouse|sql|engineer|etl|integration|cloud|query)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(800)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc_el = s.select_one(".location, .job-location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        dt = ""
        return title, loc, dt

    except:
        return "", "", ""

def extract_firebolt(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/careers/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue
        
        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
