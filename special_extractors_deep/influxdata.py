# extractor_influxdata.py

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(time series|influx|tsdb|metrics|data|pipeline|etl|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=33000, wait_until="networkidle")
        page.wait_for_timeout(750)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        el = s.select_one(".location, .job-location")
        if el:
            loc = el.get_text(" ", strip=True)

        return title, loc, ""
    except:
        return "", "", ""

def extract_influxdata(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/influxdata/job/']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
