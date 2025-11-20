# extractor_ibm.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(watson|cloud pak|data|integration|etl|pipeline|engineer|analytics)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=42000, wait_until="networkidle")
        page.wait_for_timeout(900)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc_el = s.select_one(".job-location, .ibm-card__location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        dt = ""
        return title, loc, dt
    except:
        return "", "", ""

def extract_ibm(soup, page, base_url):
    out = []

    for job in soup.select("a[href*='/job/']"):
        link = urljoin(base_url, job.get("href"))
        text = job.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, link)
        if RELEVANT.search(title):
            out.append((link, title, None))

    return out
