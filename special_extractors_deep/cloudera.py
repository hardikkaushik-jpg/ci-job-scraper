# extractor_cloudera.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(cloudera|data|hadoop|spark|etl|cloud|warehouse|integration|pipelines?)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)

        s = BeautifulSoup(page.content(), "lxml")

        h = s.find("h1")
        title = h.get_text(" ", strip=True) if h else ""

        loc = ""
        for sel in [".job-location", ".location", "[data-automation-id='location']"]:
            el = s.select_one(sel)
            if el:
                loc = el.get_text(" ", strip=True)
                break

        dt = ""
        for sc in s.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string)
                if isinstance(data, dict) and data.get("datePosted"):
                    dt = data["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt

    except:
        return "", "", ""

def extract_cloudera(soup, page, base_url):
    out = []

    # Workday job links
    for a in soup.select("a[href*='job']"):
        href = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
