# extractor_alation.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(data|engineer|etl|pipeline|integration|sql|cloud|catalog|governance)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=35000, wait_until="networkidle")
        page.wait_for_timeout(800)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        title = soup.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc = ""
        for sel in ["[data-test='job-location']", ".location", ".job-location"]:
            el = soup.select_one(sel)
            if el:
                loc = el.get_text(" ", strip=True)
                break

        # posting date via JSON-LD
        date_posted = ""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                payload = json.loads(script.string)
                if isinstance(payload, dict) and payload.get("datePosted"):
                    date_posted = payload["datePosted"].split("T")[0]
                    break
            except:
                pass

        return title, loc, date_posted

    except:
        return "", "", ""

def extract_alation(soup, page, base_url):
    out = []

    cards = soup.select("a[href*='alation.com/careers/job']")
    for a in cards:
        link = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue

        title, loc, dt = fetch_detail(page, link)

        if RELEVANT.search(title):
            out.append((link, title, None))

    return out
