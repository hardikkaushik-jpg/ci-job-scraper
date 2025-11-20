# extractor_atlan.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|integration|catalog|governance|connector|pipeline|engineer|analytics)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(700)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        title = soup.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        loc_el = soup.select_one(".job-location, .location, [data-test='location']")
        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        dt = ""
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                o = json.loads(sc.string)
                if isinstance(o, dict) and o.get("datePosted"):
                    dt = o["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt
    except:
        return "", "", ""

def extract_atlan(soup, page, base_url):
    out = []

    for a in soup.select("a[href*='atlan.com/careers/job']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
