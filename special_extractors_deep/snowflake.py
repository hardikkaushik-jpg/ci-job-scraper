# special_extractors_deep/snowflake.py — v2.0
# Snowflake careers page is JS-rendered via Phenom People platform
# Uses Playwright scroll + pagination to capture all cards

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

BASE = "https://careers.snowflake.com"

def extract_snowflake(soup, page, main_url):
    out = []
    seen = set()

    try:
        page.goto(main_url, timeout=60_000, wait_until="networkidle")
        page.wait_for_timeout(2500)

        # Scroll to load all lazy-loaded cards
        for _ in range(8):
            page.keyboard.press("End")
            page.wait_for_timeout(800)

        # Click "Load more" if present
        for _ in range(10):
            try:
                btn = page.query_selector("button[data-ph-at-id='load-more-button'], "
                                          "button:has-text('Load more'), "
                                          "button:has-text('Show more')")
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(1200)
                else:
                    break
            except Exception:
                break

        html = page.content()
    except Exception as e:
        print(f"[Snowflake] navigation error: {e}")
        html = ""

    if not html:
        return out

    s = BeautifulSoup(html, "lxml")

    # Phenom People job cards — try multiple selectors
    selectors = [
        "a[href*='/global/en/job/']",
        "a[href*='/us/en/job/']",
        "a.phs-job-list__job-title",
        "li.job-list-item a[href]",
    ]

    for sel in selectors:
        for a in s.select(sel):
            href = a.get("href", "").strip()
            if not href:
                continue
            link = href if href.startswith("http") else urljoin(BASE, href)
            if link in seen:
                continue
            seen.add(link)

            title = a.get_text(" ", strip=True)
            if not title or len(title) < 4:
                # Try sibling/parent for title
                parent = a.find_parent("li") or a.find_parent("div")
                if parent:
                    h = parent.find(["h2", "h3", "h4"])
                    if h:
                        title = h.get_text(" ", strip=True)
            if not title:
                continue

            # Location from card
            loc = ""
            parent = a.find_parent("li") or a.find_parent("div")
            if parent:
                for loc_sel in [".job-location", ".location", "[class*='location']",
                                 "span[data-ph-at-id='job-location']"]:
                    el = parent.select_one(loc_sel)
                    if el:
                        loc = el.get_text(" ", strip=True)
                        break

            out.append((link, title, "", loc, ""))

        if out:
            break  # stop trying selectors once we found results

    print(f"[Snowflake] Extracted {len(out)} jobs")
    return out
