# special_extractors_deep/datadog.py — v3.0
# Datadog uses a custom React careers page — no public Greenhouse API
# Playwright DOM scraping with relevance pre-filter

import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

CAREERS_URL = "https://careers.datadoghq.com/all-jobs/"

RELEVANT = re.compile(
    r"\b(engineer|developer|architect|scientist|analyst|sre|"
    r"platform|data|observab|monitor|pipeline|integrat|"
    r"product manager|technical|infrastructure|security|"
    r"ml|ai|machine learning|vector|embedding)\b",
    re.I
)

DROP = re.compile(
    r"\b(account executive|business development|bdr|sdr|"
    r"recruiter|talent acquisition|legal|paralegal|"
    r"executive assistant|office manager)\b",
    re.I
)

def extract_datadog(soup, page, base_url):
    out = []
    seen = set()

    try:
        page.goto(CAREERS_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(2000)
        for _ in range(5):
            page.keyboard.press("End")
            page.wait_for_timeout(800)
        for _ in range(10):
            try:
                btn = page.query_selector(
                    "button:has-text('Load more'), button:has-text('Show more')"
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
        print(f"[Datadog] render error: {e}")
        html = ""

    s = BeautifulSoup(html or "", "lxml") if html else soup

    for sel in ["a[href*='/job/']", ".job-listing a[href]", "li.opening a[href]"]:
        for a in s.select(sel):
            href = a.get("href", "").strip()
            if not href:
                continue
            link = href if href.startswith("http") else urljoin("https://careers.datadoghq.com", href)
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

            if DROP.search(title) or not RELEVANT.search(title):
                continue

            loc = ""
            parent = a.find_parent(["li", "div"])
            if parent:
                for el in parent.select("span, [class*='location']"):
                    t = el.get_text(" ", strip=True)
                    if t and re.search(r"\b(remote|usa|york|paris|london|dublin)\b", t, re.I) and len(t) < 60:
                        loc = t
                        break

            out.append((link, title, "", loc, ""))
        if out:
            break

    print(f"[Datadog] Extracted {len(out)} relevant jobs")
    return out
