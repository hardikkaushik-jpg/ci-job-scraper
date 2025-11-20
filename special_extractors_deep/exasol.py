# extractor_exasol.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(analytics|warehouse|sql|data|engineer|integration|bi)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(700)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        le = s.select_one(".location, .job-location")
        if le:
            loc = le.get_text(" ", strip=True)

        dt = ""
        return title, loc, dt

    except:
        return "", "", ""

def extract_exasol(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue
        
        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
