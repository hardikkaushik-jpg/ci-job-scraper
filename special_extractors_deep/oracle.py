# extractor_oracle.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(cloud|oci|autonomous|data|etl|integration|warehouse|analytics|engineer)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=42000, wait_until="networkidle")
        page.wait_for_timeout(1200)
        s = BeautifulSoup(page.content(), "lxml")

        title = (s.find("h1").get_text(" ", strip=True)
                 if s.find("h1") else "")

        loc_el = s.select_one(".job-location, .location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

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

def extract_oracle(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, a.get("href"))
        txt = a.get_text(" ", strip=True)

        if not RELEVANT.search(txt):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
