# special_extractors_deep/qdrant.py — v1.0
# Qdrant hosts careers on their own site (no standard ATS detected)
# Playwright render required

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

CAREERS_URL = "https://qdrant.tech/careers/"

def extract_qdrant(soup, page, main_url):
    out = []
    seen = set()

    try:
        page.goto(CAREERS_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1500)
        html = page.content()
    except Exception as e:
        print(f"[Qdrant] render error: {e}")
        html = ""

    s = BeautifulSoup(html or "", "lxml") if html else soup

    # Qdrant careers page — look for job listing links
    selectors = [
        "a[href*='/careers/']",
        "a[href*='/jobs/']",
        ".job-listing a[href]",
        ".vacancy a[href]",
        "article a[href]",
        "li a[href*='qdrant']",
    ]

    for sel in selectors:
        for a in s.select(sel):
            href = a.get("href", "").strip()
            if not href:
                continue

            # Skip nav/footer links back to /careers/ root
            if href.rstrip("/") in ("/careers", "https://qdrant.tech/careers"):
                continue

            link = href if href.startswith("http") else urljoin("https://qdrant.tech", href)

            # Must look like a specific job page, not a category
            if not re.search(r"/careers/[^/]+/[^/]+|/jobs/[a-zA-Z0-9\-]{5,}", link):
                continue

            if link in seen:
                continue
            seen.add(link)

            # Title
            title = ""
            parent = a.find_parent(["li", "article", "div"])
            if parent:
                h = parent.find(["h2", "h3", "h4"])
                if h:
                    title = h.get_text(" ", strip=True)
            if not title:
                title = a.get_text(" ", strip=True)
            if not title or len(title) < 4:
                continue

            # Location
            loc = ""
            if parent:
                for loc_sel in [".location", "[class*='location']", "span"]:
                    els = parent.select(loc_sel)
                    for el in els:
                        t = el.get_text(" ", strip=True)
                        if t and re.search(
                            r"\b(remote|germany|berlin|usa|uk|europe|hybrid)\b", t, re.I
                        ):
                            loc = t
                            break
                    if loc:
                        break

            out.append((link, title, "", loc, ""))
        if out:
            break

    # If no structured job pages found, look for any anchor with job-like text
    if not out:
        for a in s.find_all("a", href=True):
            href = a.get("href", "").strip()
            text = a.get_text(" ", strip=True)
            if not href or not text:
                continue
            if not re.search(
                r"\b(engineer|developer|manager|analyst|scientist|designer|lead|intern)\b",
                text, re.I
            ):
                continue
            link = href if href.startswith("http") else urljoin("https://qdrant.tech", href)
            if link in seen or "qdrant" not in link.lower():
                continue
            seen.add(link)
            out.append((link, text, "", "", ""))

    print(f"[Qdrant] Extracted {len(out)} jobs")
    return out
