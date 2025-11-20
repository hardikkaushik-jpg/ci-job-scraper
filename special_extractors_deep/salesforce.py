# extractor_salesforce.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(mulesoft|tableau|integration|data|analytics|engineer|etl|api|platform)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1400)

        s = BeautifulSoup(page.content(), "lxml")

        title = (s.find("h1").get_text(" ", strip=True)
                 if s.find("h1") else "")

        loc = ""
        le = s.select_one(".job-location, .location")
        if le: loc = le.get_text(" ", strip=True)

        dt = ""
        for js in s.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(js.string)
                if d.get("datePosted"):
                    dt = d["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt
    except:
        return "", "", ""

def extract_salesforce(soup, page, base_url):
    out = []

    for row in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, row.get("href"))
        text = row.get_text(" ", strip=True)

        if not RELEVANT.search(text): continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
