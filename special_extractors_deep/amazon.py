# extractor_amazon.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(aws|redshift|glue|kinesis|data|etl|lake|pipeline|integration)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        title = soup.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        location = ""
        loc_el = soup.select_one(".job-info span")
        if loc_el:
            location = loc_el.get_text(" ", strip=True)

        # posted date
        dt = ""
        dt_el = soup.find("span", {"class": "posting-date"})
        if dt_el:
            dt = dt_el.get_text(" ", strip=True)

        return title, location, dt

    except:
        return "", "", ""

def extract_amazon(soup, page, base_url):
    out = []

    job_cards = soup.select("div.job-tile, a[href*='/jobs/']")
    for card in job_cards:
        a = card.find("a", href=True)
        if not a:
            continue

        link = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, link)

        if RELEVANT.search(title):
            out.append((link, title, None))

    return out
