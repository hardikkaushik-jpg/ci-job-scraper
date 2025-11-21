# qlik.py — FINAL CSOD EXTRACTOR for Qlik
# Handles full JS load, auto-scroll, and CSOD pagination.

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time, re

def extract_qlik(soup, page, base_url):
    results = []
    seen = set()

    # ==============================
    # 1. Load main page fully
    # ==============================
    try:
        page.goto(base_url, wait_until="networkidle", timeout=60000)
        time.sleep(1.0)
    except Exception as e:
        print("[QLIK] Initial load failed:", e)
        return []

    # =========================================
    # 2. Scroll to the bottom (CSOD lazy loads)
    # =========================================
    last_height = -1
    for _ in range(15):  # scroll enough for 100+ jobs
        page.mouse.wheel(0, 2500)
        time.sleep(1.0)

        height = page.evaluate("() => document.body.scrollHeight")
        if height == last_height:
            break
        last_height = height

    html = page.content()
    soup = BeautifulSoup(html, "lxml")

    # =========================================
    # 3. Extract job cards
    # =========================================
    cards = soup.select("a.career-site-job, a[href*='/job/']")
    if not cards:
        cards = soup.find_all("a", href=True)

    for a in cards:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # ----------------- TITLE -----------------
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 2:
            # Sometimes title is outside <a>
            parent = a.find_parent()
            if parent:
                title = parent.get_text(" ", strip=True)

        if not title or len(title) < 2:
            continue

        # ---------------- LOCATION ----------------
        loc = ""
        parent = a.find_parent()
        if parent:
            loc_el = (parent.select_one(".location")
                      or parent.select_one(".job-location")
                      or parent.select_one(".posting-location")
                      or parent.select_one("span[class*='location']")
                      or parent.select_one("div[class*='meta']"))

            if loc_el:
                loc = loc_el.get_text(" ", strip=True)

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
