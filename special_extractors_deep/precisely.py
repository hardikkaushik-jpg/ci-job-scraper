# precisely.py
# Deep extractor for Precisely US + International career pages

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time, re


def extract_precisely(soup, page, base_url):
    results = []
    seen = set()

    # --- Load full JS-rendered page ---
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)

        # Scrolling — Precisely loads jobs as you scroll
        for _ in range(5):
            page.mouse.wheel(0, 1800)
            time.sleep(0.7)

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

    except Exception:
        pass

    # ================================
    # Precisely job card selectors
    # ================================
    selectors = [
        "div.careers-job-listing",           # common wrapper
        "div.job",                           # generic
        "li.job",                            # list format
        "div.position",                      # fallback
        "a.careers-job-listing",             # direct links
        "a[href*='jobs']",                   # ATS links
    ]

    job_nodes = []
    for sel in selectors:
        job_nodes.extend(soup.select(sel))

    # also catch anchors inside containers
    job_nodes.extend(soup.select("div.careers-job-listing a[href]"))

    for node in job_nodes:
        href = node.get("href")

        # sometimes nested inside <a>
        if not href:
            a = node.find("a", href=True)
            if a:
                href = a["href"]

        if not href:
            continue

        link = urljoin(base_url, href)

        # Skip noise
        skip_words = ["privacy", "about", "news", "events", "blog"]
        if any(sw in link.lower() for sw in skip_words):
            continue

        if link in seen:
            continue
        seen.add(link)

        # ---------- Extract title ----------
        title = ""

        for tag in ["h2", "h3", "h4"]:
            t = node.find(tag)
            if t:
                title = t.get_text(" ", strip=True)
                break

        if not title:
            title = node.get_text(" ", strip=True)

        if not title or len(title) < 3:
            continue

        # ---------- Extract location ----------
        location = ""

        # directly labelled nodes
        loc_el = (node.find("span", class_=re.compile("location", re.I)) or
                  node.find("div", class_=re.compile("location", re.I)) or
                  node.find("p", class_=re.compile("location", re.I)))

        if loc_el:
            location = loc_el.get_text(" ", strip=True)

        # fallback: detect " - " split
        if not location and " - " in title:
            parts = title.split(" - ")
            if len(parts) == 2:
                title, location = parts[0], parts[1]

        # combine
        label = f"{title} ({location})" if location else title

        results.append((link, label))

    return results
