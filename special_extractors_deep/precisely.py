# extract_precisely.py
# Deep extractor for Precisely careers portals

import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

RELEVANT = re.compile(
    r"(data|engineer|developer|etl|integration|product|cloud|analytics|software|platform|architect|manager)",
    re.I
)

def extract_precisely(soup, page, base_url):
    """
    Extract job title + links from Precisely career pages.
    Works for both:
        - US jobs
        - International jobs
    """

    out = []

    # All job cards use consistent structure: <a class="job-link"> or deep link inside buttons
    for job in soup.select("a, div.job-item, div.career-item, li, div"):
        # Filter realistic job anchors
        if not job:
            continue

        title = job.get_text(" ", strip=True)
        if not title:
            continue

        # Must contain job-relevant words
        if not RELEVANT.search(title):
            continue

        # Find href in <a> or nested <a>
        href = None
        if job.name == "a" and job.get("href"):
            href = job["href"]
        else:
            a = job.find("a", href=True)
            if a:
                href = a["href"]

        if not href:
            continue

        # Convert to absolute
        href = urljoin(base_url, href)

        # Typical non-job noise to eliminate
        if any(x in href.lower() for x in ["linkedin", "glassdoor", "youtube", "privacy", "culture"]):
            continue

        # Ensure we only pick actual job pages (Precicely uses greenhouse links)
        if "boards.greenhouse" not in href.lower():
            continue

        out.append((href, title))

    return out
