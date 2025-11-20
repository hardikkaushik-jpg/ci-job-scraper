# dataworld.py
# Deep extractor for Data.World careers page

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time, re

def extract_dataworld(soup, page, base_url):
    results = []
    seen = set()

    # --- Dynamic load + scroll ---
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)
        for _ in range(3):
            page.mouse.wheel(0, 1400)
            time.sleep(0.7)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    # root job list container
    root = soup.find(id="careers-list")
    if not root:
        root = soup  # fallback

    job_cards = root.select("a[href]") + root.select("div.job") + root.select("div.opening")

    for node in job_cards:
        href = node.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)

        # skip non-job navigation anchors
        if any(bad in link.lower() for bad in ["#","/company","/about","/blog"]):
            continue

        if link in seen:
            continue
        seen.add(link)

        # title
        title = (
            node.get_text(" ", strip=True)
            or (node.find("h2").get_text(" ", strip=True) if node.find("h2") else "")
            or (node.find("h3").get_text(" ", strip=True) if node.find("h3") else "")
        )

        if not title or len(title) < 3:
            continue

        # location
        location = ""
        loc_el = (
            node.find("span", class_=re.compile("location", re.I))
            or node.find("p")
            or node.find("div", class_=re.compile("location", re.I))
        )
        if loc_el:
            location = loc_el.get_text(" ", strip=True)

        label = f"{title} ({location})" if location else title
        results.append((link, label))

    return results
