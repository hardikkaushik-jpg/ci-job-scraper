# special_extractors_deep/ibm.py — v2.0
# IBM Phenom People platform
# Added: relevance pre-filter, 5-tuple output, load-more pagination

import re
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.ibm.com/careers/search"

RELEVANT = re.compile(
    r"\b(data|etl|integration|pipeline|engineer|analyst|architect|"
    r"cloud|platform|bi|analytics|database|sql|developer|sre|"
    r"integration|connector|governance|observ)\b",
    re.I
)

def extract_ibm(soup, page, base_url):
    out = []
    seen = set()

    # JS render with scroll and load-more
    try:
        page.goto(base_url, wait_until="networkidle", timeout=55000)
        page.wait_for_timeout(2000)

        for _ in range(5):
            page.mouse.wheel(0, 1500)
            page.wait_for_timeout(600)

        for _ in range(8):
            try:
                btn = page.query_selector(
                    "button:has-text('Load more'), "
                    "button[data-ph-at-id='load-more-button'], "
                    "button:has-text('Show more')"
                )
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(1500)
                else:
                    break
            except Exception:
                break

        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        print(f"[IBM] render error: {e}")

    selectors = [
        "a[href*='/job/']",
        "a[href*='/jobs/']",
        "a[href*='jobId=']",
        "div[data-ph-at-id='job-card'] a[href]",
        ".job-list-item a[href]",
    ]

    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            if not href:
                continue

            # Skip search/filter pages
            if "search" in href.lower() and "job" not in href.lower():
                continue

            link = urljoin(base_url, href)
            if link in seen:
                continue
            seen.add(link)

            # Title
            title = a.get_text(" ", strip=True)
            if not title or len(title) < 3:
                parent = a.find_parent(["div", "li"])
                if parent:
                    h = parent.find(["h2", "h3", "h4"])
                    if h:
                        title = h.get_text(" ", strip=True)
            if not title or len(title) < 3:
                continue

            # Relevance filter — IBM posts thousands of unrelated roles
            if not RELEVANT.search(title):
                continue

            # Location
            loc = ""
            card = (
                a.find_parent("div", class_=re.compile(r"(job|card|result|listing)", re.I))
                or a.find_parent("li")
            )
            if card:
                for loc_sel in [".job-location", ".location",
                                 "span[class*='location']", "p"]:
                    el = card.select_one(loc_sel)
                    if el:
                        candidate = el.get_text(" ", strip=True)
                        if len(candidate) < 80:
                            loc = candidate
                            break

            out.append((link, title, "", loc, ""))

    print(f"[IBM] Extracted {len(out)} jobs")
    return out
