# alation.py
# Special deep extractor for Alation (Workday tenant)
# Same logic style as Cloudera extractor

import json
from bs4 import BeautifulSoup
import re

def extract_alation(soup, page, base_url):
    results = []
    seen = set()

    # Workday uses <script> with "wd-jobs" JSON payload
    scripts = soup.find_all("script")
    for script in scripts:
        if not script.string:
            continue
        txt = script.string.strip()

        # Detect Workday JSON (usually contains "jobPostings" or "wd" token)
        if '"jobPostings"' not in txt and '"jobPostingInfo"' not in txt:
            continue

        try:
            data = json.loads(txt)
        except Exception:
            continue

        # Two possible entry formats:
        # - data["jobPostings"]
        # - data["jobPostingInfo"]
        jobs = []

        if isinstance(data, dict):
            if "jobPostings" in data:
                jobs = data["jobPostings"]
            elif "jobPostingInfo" in data:
                jobs = data["jobPostingInfo"]
        if not jobs:
            continue

        for job in jobs:
            link = job.get("externalUrl") or job.get("externalPath") or job.get("detailUrl")
            title = job.get("title") or job.get("displayJobTitle")

            if not link or not title:
                continue

            # Normalize link
            if link.startswith("/"):
                link = base_url.rstrip("/") + link

            if link in seen:
                continue
            seen.add(link)

            # Clean title
            title = re.sub(r'\s+', ' ', title).strip()

            # Skip garbage rows
            if not title or len(title) < 2:
                continue
            if "Alation" in title and len(title.split()) < 3:
                continue

            results.append((link, title))

    return results
