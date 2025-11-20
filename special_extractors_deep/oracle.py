# oracle.py
# Deep extractor for Oracle ORC job boards
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time, re

def extract_oracle(soup, page, base_url):
    results = []
    seen = set()

    # --- Force JS rendering + scrolling ---
    try:
        page.goto(base_url, wait_until="networkidle", timeout=45000)
        for _ in range(3):
            page.mouse.wheel(0, 1400)
            time.sleep(0.8)

        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        pass

    job_nodes = []

    selectors = [
        "a[href*='/job/']",
        "a[href*='/jobs/']",
        ".job", ".job-card", ".job-item",
        ".card a[href]",
        "div[data-qa='search-result'] a[href]",
        "li a[href*='/job/']",
    ]

    for sel in selectors:
        job_nodes.extend(soup.select(sel))

    for a in job_nodes:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # ----- Extract Title -----
        title = a.get_text(" ", strip=True)

        # If anchor is empty, try parents
        if not title or len(title) < 2:
            parent = a.find_parent()
            if parent:
                t2 = parent.get_text(" ", strip=True)
                if t2 and len(t2) > 2:
                    title = t2

        title = title.strip()
        if len(title) < 2:
            continue

        # ----- Extract Location -----
        location = ""

        card = (
            a.find_parent("div", class_=re.compile("(job|card|item)", re.I))
            or a.find_parent("li")
            or a.find_parent("article")
        )

        if card:
            loc_el = (
                card.select_one(".job-location")
                or card.select_one(".location")
                or card.select_one("span.location")
                or card.find("p")
            )
            if loc_el:
                location = loc_el.get_text(" ", strip=True)

        # fallback: extract parent paragraphs
        if not location:
            parent = a.find_parent()
            if parent:
                loc_el = parent.find("p")
                if loc_el:
                    location = loc_el.get_text(" ", strip=True)

        # final label
        label = f"{title} ({location})" if location else title
        results.append((link, label))

    return
