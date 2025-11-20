# extractor_dataworld.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|engineer|analytics|cloud|etl|integration|sql|catalog)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(800)
        s = BeautifulSoup(page.content(), "lxml")

        title = s.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = s.select_one(".job-location, .location")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        for js in s.find_all("script", type="application/ld+json"):
            try:
                o = json.loads(js.string)
                if o.get("datePosted"):
                    dt = o["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt

    except:
        return "", "", ""

def extract_dataworld(soup, page, base_url):
    out = []

    for card in soup.select("div.careers-listing a[href]"):
        href = urljoin(base_url, card.get("href"))
        text = card.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue
        
        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
