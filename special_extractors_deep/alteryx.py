# alteryx.py
# Correct deep extractor for Alteryx (Workday ATS)
# Uses Workday cxs API + pagination

import json
from urllib.parse import urljoin

def extract_alteryx(soup, page, base_url):
    results = []
    seen = set()

    # Convert:
    # https://alteryx.wd108.myworkdayjobs.com/en-US/AlteryxCareers
    # → https://alteryx.wd108.myworkdayjobs.com/wday/cxs/alteryx/AlteryxCareers/jobs
    try:
        parts = base_url.split(".com/")
        domain = parts[0] + ".com"
        tenant = parts[1].split("/")[0]          # wd108.myworkdayjobs.com/en-US → "wd108.myworkdayjobs.com"
        site = parts[1].split("/")[1]            # "AlteryxCareers"
        api_root = f"{domain}/wday/cxs/alteryx/{site}/jobs"
    except Exception:
        return []

    offset = 0
    limit = 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"

        try:
            page.goto(api_url, timeout=30000, wait_until="networkidle")
            raw = page.content()
        except:
            break

        # Workday API returns JSON inside <pre>
        try:
            start = raw.index("{")
            json_raw = raw[start:]
            data = json.loads(json_raw)
        except:
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
