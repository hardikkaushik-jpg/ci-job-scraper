# alation.py — FINAL WORKDAY EXTRACTOR (stable & correct)

import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup

def extract_alation(soup, page, base_url):
    results = []
    seen = set()

    # -------------------------------
    # 1) Build API root safely
    # -------------------------------
    # Example:
    # https://alation.wd503.myworkdayjobs.com/ExternalSite
    try:
        domain = base_url.split(".com/")[0] + ".com"
        site = base_url.rstrip("/").split("/")[-1]   # "ExternalSite"
        api_root = f"{domain}/wday/cxs/alation/{site}/jobs"
    except Exception:
        return results

    # -------------------------------
    # 2) Pagination
    # -------------------------------
    offset = 0
    limit = 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"

        try:
            page.goto(api_url, timeout=25000, wait_until="networkidle")
            raw_html = page.content()
        except Exception:
            break

        # Extract JSON from <pre> safely
        try:
            s = BeautifulSoup(raw_html, "lxml")
            pre = s.find("pre")
            if not pre:
                break
            data = json.loads(pre.get_text())
        except Exception:
            break

        postings = data.get("jobPostings", [])
        if not postings:
            break

        # -------------------------------
        # 3) Parse each job
        # -------------------------------
        for job in postings:
            link = job.get("externalPath") or job.get("externalUrl")
            title = job.get("title")

            if not link or not title:
                continue

            # Normalize URL
            link = urljoin(base_url, link)

            if link in seen:
                continue
            seen.add(link)

            results.append((link, title.strip()))

        offset += limit

    return results
