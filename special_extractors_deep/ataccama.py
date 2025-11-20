# extractor_ataccama.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|quality|governance|integration|catalog|engineer|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=32000, wait_until="networkidle")
        page.wait_for_timeout(800)

        html = page.content()
        s = BeautifulSoup(html, "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = s.select_one(".position-info__location, .job-location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        for sc in s.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(sc.string)
                if d.get("datePosted"):
                    dt = d["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt

    except:
        return "", "", ""

def extract_ataccama(soup, page, base_url):
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
