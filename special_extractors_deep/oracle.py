# special_extractors_deep/oracle.py — v2.0
# Fixed: missing `return out` on last line of original
# Improved: 5-tuple output, better title/location extraction, relevance pre-filter

import re
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

RELEVANT = re.compile(
    r"\b(data|etl|integration|pipeline|engineer|analyst|architect|"
    r"cloud|platform|bi|analytics|database|sql|developer|sre)\b",
    re.I
)

def extract_oracle(soup, page, base_url):
    out = []
    seen = set()

    # JS-render with scroll
    try:
        page.goto(base_url, wait_until="networkidle", timeout=50000)
        for _ in range(4):
            page.mouse.wheel(0, 1400)
            time.sleep(0.7)
        # Try "Load more" button
        for _ in range(5):
            try:
                btn = page.query_selector(
                    "button:has-text('Load more'), button:has-text('Show more'), "
                    "a:has-text('Load more')"
                )
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(1.2)
                else:
                    break
            except Exception:
                break
        html = page.content()
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        print(f"[Oracle] render error: {e}")

    selectors = [
        "a[href*='/job/']",
        "a[href*='/jobs/']",
        "div[data-qa='search-result'] a[href]",
        ".job-card a[href]",
        ".card a[href]",
        "li a[href*='/job/']",
    ]

    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            if not href:
                continue
            link = urljoin(base_url, href)
            if link in seen:
                continue
            seen.add(link)

            # Title
            title = a.get_text(" ", strip=True)
            if not title or len(title) < 3:
                parent = a.find_parent(["div", "li", "article"])
                if parent:
                    h = parent.find(["h2", "h3", "h4"])
                    if h:
                        title = h.get_text(" ", strip=True)

            if not title or len(title) < 3:
                continue

            # Pre-filter noise
            if not RELEVANT.search(title):
                continue

            # Location
            loc = ""
            card = (
                a.find_parent("div", class_=re.compile(r"(job|card|item)", re.I))
                or a.find_parent("li")
                or a.find_parent("article")
            )
            if card:
                for loc_sel in [".job-location", ".location", "span.location", "p"]:
                    el = card.select_one(loc_sel)
                    if el:
                        candidate = el.get_text(" ", strip=True)
                        if len(candidate) < 60:
                            loc = candidate
                            break

            out.append((link, title, "", loc, ""))

    print(f"[Oracle] Extracted {len(out)} jobs")
    return out  # was missing in original!
