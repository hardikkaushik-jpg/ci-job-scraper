# extractor_decube.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|engineer|developer|analytics|etl|sql|pipeline)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=28000, wait_until="networkidle")
        page.wait_for_timeout(600)

        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc_el = s.select_one(".location, .job-location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        dt = ""
        return title, loc, dt
        
    except:
        return "", "", ""

def extract_decube(soup, page, base_url):
    out = []

    for job in soup.select("a[href*='/jobs/']"):
        href = urljoin(base_url, job.get("href"))
        text = job.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue
        
        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
