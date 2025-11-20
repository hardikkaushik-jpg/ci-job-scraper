# ibm.py
# Deep extractor for IBM (BrassRing / IBM Talent Platform)

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time, json, re

def extract_ibm(soup, page, base_url):
    results = []
    seen = set()

    # --- JS render + scroll ---
    try:
        page.goto(base_url, wait_until="networkidle", timeout=50000)
        for _ in range(4):
            page.mouse.wheel(0, 1500)
            time.sleep(0.7)
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    # --- Find job cards ---
    selectors = [
        "a[href*='/job/']",
        "a[href*='/jobs/']",
        "a[href*='jobId=']",
        "div[data-ph-at-id='job-card'] a[href]",
        ".job", ".job-card", ".card", ".job-list-item"
    ]

    nodes = []
    for sel in selectors:
        nodes.extend(soup.select(sel))

    for a in nodes:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)

        # skip search / filter urls
        if "search" in link.lower() and "job" not in link.lower():
            continue

        if link in seen:
            continue
        seen.add(link)

        # --- extract title ---
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 2:
            # fallback: parent heading
            parent = a.find_parent(["div", "li"])
            if parent:
                h = parent.find(["h2", "h3", "h4"])
                if h:
                    title = h.get_text(" ", strip=True)

        if not title or len(title) < 2:
            continue

        # --- extract location ---
        location = ""

        card = (
            a.find_parent("div", class_=re.compile("(job|card|result|listing)", re.I))
            or a.find_parent("li")
        )

        if card:
            loc_el = (
                card.select_one(".job-location")
                or card.select_one(".location")
                or card.find("span", class_=re.compile("location", re.I))
                or card.find("p")
            )
            if loc_el:
                location = loc_el.get_text(" ", strip=True)

        # location fallback: parent container paragraphs
        if not location:
            parent = a.find_parent()
            if parent:
                p = parent.find("p")
                if p:
                    location = p.get_text(" ", strip=True)

        # --- IBM embeds data in JSON in <script> tags ---
        if not location:
            for script in soup.find_all("script"):
                if not script.string:
                    continue
                if "jobLocation" in script.string:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict):
                            jl = data.get("jobLocation")
                            if jl and isinstance(jl, dict):
                                loc = jl.get("addressLocality")
                                country = jl.get("addressCountry")
                                if loc:
                                    location = f"{loc}, {country or ''}".strip(", ")
                    except:
                        pass

        label = f"{title} ({location})" if location else title
        results.append((link, label))

    return results
