# extractor_couchbase.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(couchbase|nosql|database|data|cloud|engineer|integration|pipelines?)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=36000, wait_until="networkidle")
        page.wait_for_timeout(750)

        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = s.select_one(".job-location, .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        for sc in s.find_all("script", type="application/ld+json"):
            try:
                obj = json.loads(sc.string)
                if obj.get("datePosted"):
                    dt = obj["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt
    except:
        return "", "", ""

def extract_couchbase(soup, page, base_url):
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
