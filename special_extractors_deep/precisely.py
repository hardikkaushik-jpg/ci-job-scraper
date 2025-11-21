# precisely.py — FINAL VERSION (for your 2 URLs only)
# Handles dynamic scrolling + custom Precisely page structure

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def extract_precisely(soup, page, base_url):
    results = []
    seen = set()

    # ==============================
    # 1. FULL JS LOAD + SCROLL
    # ==============================
    try:
        page.goto(base_url, wait_until="networkidle", timeout=60000)

        # Precisely loads jobs after scrolling
        last_height = -1
        for _ in range(12):  # scroll ~12 times (enough for 100+ jobs)
            page.mouse.wheel(0, 2000)
            time.sleep(1.0)

            # stop if no more scrolling
            curr_height = page.evaluate("() => document.body.scrollHeight")
            if curr_height == last_height:
                break
            last_height = curr_height

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

    except Exception as e:
        print("[PRECISELY] JS load failed:", e)
        return []

    # ====================================
    # 2. JOB CARD SELECTORS
    # ====================================
    selectors = [
        "div.careers-job-listing",
        "li.careers-job-listing",
        "div.job",
        "li.job",
        "div.position",
        "a.careers-job-listing",
        "a[href*='/job/']",
    ]

    job_nodes = []
    for sel in selectors:
        job_nodes.extend(soup.select(sel))

    # also catch anchors inside nodes
    job_nodes.extend(soup.select("div.careers-job-listing a[href]"))

    # ====================================
    # 3. PARSE JOB CARDS
    # ====================================
    for node in job_nodes:

        # -------- Extract link --------
        href = node.get("href")

        # sometimes nested
        if not href:
            a = node.find("a", href=True)
            if a:
                href = a["href"]

        if not href:
            continue

        link = urljoin(base_url, href)

        if link in seen:
            continue
        seen.add(link)

        # -------- Extract title --------
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

        # -------- Extract location --------
        loc = ""
        loc_el = (
            node.find("span", class_=lambda x: x and "location" in x.lower()) or
            node.find("div", class_=lambda x: x and "location" in x.lower()) or
            node.find("p", class_=lambda x: x and "location" in x.lower())
        )

        if loc_el:
            loc = loc_el.get_text(" ", strip=True)

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
