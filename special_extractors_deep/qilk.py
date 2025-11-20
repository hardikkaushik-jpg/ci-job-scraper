# extractor_qlik.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(qlik|data|etl|bi|analytics|engineer|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=38000, wait_until="networkidle")
        page.wait_for_timeout(900)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        le = s.select_one(".job-location, .location")
        if le: loc = le.get_text(" ", strip=True)

        return title, loc, ""
    except:
        return "", "", ""

def extract_qlik(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text): 
            continue

        title, loc, _ = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
