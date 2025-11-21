# alation.py
# REAL Workday extractor for Alation
# Uses Workday's fs/search API + pagination
# Same style as Cloudera

import json
from urllib.parse import urljoin

def extract_alation(soup, page, base_url):
    results = []
    seen = set()

    # Convert:
    # https://alation.wd503.myworkdayjobs.com/ExternalSite
    # → API root:
    # https://alation.wd503.myworkdayjobs.com/wday/cxs/alation/ExternalSite/jobs
    try:
        parts = base_url.split(".com/")
        api_base = parts[0] + ".com/wday/cxs/"  # domain
        tail = parts[1].split("/")[1]          # "ExternalSite"
        api_root = api_base + "alation/" + tail + "/jobs"
    except Exception:
        return []

    # Paginated API
    offset = 0
    limit = 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"

        try:
            page.goto(api_url, timeout=30000, wait_until="networkidle")
            raw = page.content()
        except Exception:
            break

        # Workday API is JSON inside <pre> tag
        try:
            start = raw.index("{")
            json_raw = raw[start:]
            data = json.loads(json_raw)
        except Exception:
            break

        # No jobs? End.
        if "jobPostings" not in data or not data["jobPostings"]:
            break

        # Process each job
        for job in data["jobPostings"]:
            link = job.get("externalPath") or job.get("externalUrl")
            title = job.get("title")

            if not link or not title:
                continue

            # Normalize link
            if link.startswith("/"):
                link = urljoin(base_url, link)

            if link in seen:
                continue
            seen.add(link)

            # Clean title
            title = " ".join(title.split()).strip()

            # Filter garbage
            if len(title) < 2:
                continue

            results.append((link, title))

        # Move to next page
        offset += limit

    return results
