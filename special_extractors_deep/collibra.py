# extractor_collibra.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|governance|catalog|quality|integration|engineer|platform|cloud)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=38000, wait_until="networkidle")
        page.wait_for_timeout(700)

        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = s.select_one(".location, .job-location")
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

def extract_collibra(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='collibra.com/careers/job']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
