# extractor_datadog.py

import re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(r"(observability|metrics|monitor|logs|apm|sre|devops|pipeline|etl|data)", re.I)

def fetch_detail(page, link):
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        soup = BeautifulSoup(page.content(), "lxml")

        title = soup.find("h1")
        title = title.get_text(" ", strip=True) if title else ""

        loc_el = soup.select_one(".job-location, .location")
        loc = loc_el.get_text(" ", strip=True) if loc_el else ""

        dt = ""
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string)
                if data.get("datePosted"):
                    dt = data["datePosted"].split("T")[0]
            except:
                pass

        return title, loc, dt
    except:
        return "", "", ""

def extract_datadog(soup, page, base_url):
    out = []

    for job in soup.select("a[href*='/job/']"):
        href = urljoin(base_url, job.get("href"))
        text = job.get_text(" ", strip=True)

        if not RELEVANT.search(text):
            continue
        
        title, loc, dt = fetch_detail(page, href)
        if RELEVANT.search(title):
            out.append((href, title, None))

    return out
