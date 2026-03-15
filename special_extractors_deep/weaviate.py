# special_extractors_deep/weaviate.py — v1.0
# Weaviate uses Welcome to the Jungle (WTTJ)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

WTTJ_BASE   = "https://www.welcometothejungle.com"
WEAVIATE_URL = "https://www.welcometothejungle.com/en/companies/weaviate/jobs"

def extract_weaviate(soup, page, main_url):
    out = []
    seen = set()

    try:
        page.goto(WEAVIATE_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1500)
        html = page.content()
    except Exception as e:
        print(f"[Weaviate] render error: {e}")
        html = ""

    s = BeautifulSoup(html or "", "lxml") if html else soup

    selectors = [
        "a[href*='/en/companies/weaviate/jobs/']",
        "li[data-testid='job-card'] a",
        ".sc-job-card a[href]",
        "a[href*='/jobs/'][href*='weaviate']",
    ]

    for sel in selectors:
        for a in s.select(sel):
            href = a.get("href", "").strip()
            if not href:
                continue
            link = href if href.startswith("http") else urljoin(WTTJ_BASE, href)
            if link in seen:
                continue
            seen.add(link)

            # Title from h3 inside card, fallback to anchor text
            title = ""
            parent = a.find_parent("li") or a.find_parent("div")
            if parent:
                h = parent.find(["h3", "h2", "h4"])
                if h:
                    title = h.get_text(" ", strip=True)
            if not title:
                title = a.get_text(" ", strip=True)
            if not title or len(title) < 4:
                continue

            # Location
            loc = ""
            if parent:
                for loc_sel in ["[data-testid='job-location']", ".location",
                                 "span[class*='location']"]:
                    el = parent.select_one(loc_sel)
                    if el:
                        loc = el.get_text(" ", strip=True)
                        break

            # Date
            posting_date = ""
            if parent:
                time_el = parent.select_one("time[datetime]")
                if time_el:
                    posting_date = (time_el.get("datetime") or "").split("T")[0]

            out.append((link, title, "", loc, posting_date))
        if out:
            break

    print(f"[Weaviate WTTJ] Extracted {len(out)} jobs")
    return out
