# alteryx.py — FINAL WORKING VERSION
# Workday cxs API Scraper for Alteryx

import json
from urllib.parse import urljoin

def extract_alteryx(soup, page, base_url):
    results = []
    seen = set()

    # -----------------------------------------------------
    # Build API root
    # Base URL example:
    #   https://alteryx.wd108.myworkdayjobs.com/en-US/AlteryxCareers
    #
    # Workday API root must be:
    #   https://alteryx.wd108.myworkdayjobs.com/wday/cxs/alteryx/AlteryxCareers/jobs
    # -----------------------------------------------------
    try:
        domain = base_url.split(".com")[0] + ".com"
        site = base_url.rstrip("/").split("/")[-1]      # "AlteryxCareers"
        tenant = "alteryx"                               # constant for Alteryx

        api_root = f"{domain}/wday/cxs/{tenant}/{site}/jobs"

    except Exception as e:
        print("[ALTERYX] Failed to build API root:", e)
        return []

    offset = 0
    limit = 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"

        try:
            page.goto(api_url, timeout=30000, wait_until="networkidle")

            # Workday JSON is inside a <pre> tag
            try:
                raw_json = page.inner_text("pre")
            except:
                raw_json = page.content()

        except Exception:
            break

        try:
            data = json.loads(raw_json)
        except Exception:
            break

        postings = data.get("jobPostings", [])
        if not postings:
            break

        for job in postings:
            title = job.get("title")
            link = job.get("externalPath") or job.get("externalUrl")

            if not title or not link:
                continue

            if link.startswith("/"):
                link = urljoin(base_url, link)

            if link in seen:
                continue
            seen.add(link)

            title = " ".join(title.split()).strip()
            results.append((link, title))

        offset += limit

    return results
