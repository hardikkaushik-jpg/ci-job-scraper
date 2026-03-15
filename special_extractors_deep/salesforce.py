# special_extractors_deep/salesforce.py — v2.0
# Salesforce uses Phenom People platform
# API endpoint discovered from network inspection of careers.salesforce.com

import requests
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://careers.salesforce.com"

# Salesforce uses Phenom People — their search API
PHENOM_API = (
    "https://careers.salesforce.com/en/jobs/?search=data+integration"
    "&country=&location=&department=&type="
)

# Relevant title filter — Salesforce is huge, we only want CI-relevant roles
import re
RELEVANT = re.compile(
    r"\b(data|etl|integration|pipeline|mulesoft|tableau|analyst|"
    r"engineer|architect|platform|database|cloud|bi|analytics|crm)\b",
    re.I
)

def extract_salesforce(soup, page, base_url):
    out = []
    seen = set()

    # Salesforce careers is a React SPA — Playwright render required
    try:
        page.goto(base_url, timeout=60000, wait_until="networkidle")
        page.wait_for_timeout(2000)

        # Scroll to load lazy cards
        for _ in range(6):
            page.keyboard.press("End")
            page.wait_for_timeout(700)

        # Load more if button exists
        for _ in range(8):
            try:
                btn = page.query_selector(
                    "button:has-text('Load more'), "
                    "button[data-ph-at-id='load-more-button'], "
                    "a:has-text('Show more jobs')"
                )
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(1200)
                else:
                    break
            except Exception:
                break

        html = page.content()
    except Exception as e:
        print(f"[Salesforce] render error: {e}")
        html = ""

    if html:
        s = BeautifulSoup(html, "lxml")
    else:
        s = soup

    # Phenom People job card selectors
    selectors = [
        "a[href*='/en/jobs/']",
        "a[href*='/job/']",
        ".phs-job-list__job-title",
        "li.job-list-item a[href]",
        "a[data-ph-at-id='job-link']",
    ]

    for sel in selectors:
        for a in s.select(sel):
            href = a.get("href", "").strip()
            if not href:
                continue
            link = href if href.startswith("http") else urljoin(BASE_URL, href)
            if link in seen:
                continue
            seen.add(link)

            title = a.get_text(" ", strip=True)
            if not title:
                parent = a.find_parent(["li", "div"])
                if parent:
                    h = parent.find(["h2", "h3", "h4"])
                    if h:
                        title = h.get_text(" ", strip=True)
            if not title or len(title) < 4:
                continue

            # Relevance filter — Salesforce has thousands of unrelated roles
            if not RELEVANT.search(title):
                continue

            # Location
            loc = ""
            parent = a.find_parent(["li", "div"])
            if parent:
                for loc_sel in [".location", "[class*='location']",
                                 "span[data-ph-at-id='job-location']",
                                 ".job-location"]:
                    el = parent.select_one(loc_sel)
                    if el:
                        loc = el.get_text(" ", strip=True)
                        break

            out.append((link, title, "", loc, ""))
        if out:
            break

    print(f"[Salesforce] Extracted {len(out)} jobs")
    return out
