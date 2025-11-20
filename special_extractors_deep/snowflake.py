# snowflake.py
# Deep extractor for Snowflake careers page (JS-loaded job cards)

from urllib.parse import urljoin
import time

def extract_snowflake(soup, page, base_url):
    """
    Extract jobs from Snowflake careers search results page.
    Uses Playwright page object to wait for job cards to load, then parse.
    """
    out = []

    # Navigate to base_url (overwrite soup as needed)
    try:
        page.goto(base_url, timeout=60000, wait_until="networkidle")
        # Wait a little longer for JS to load job cards
        page.wait_for_timeout(2000)
    except Exception:
        # If navigation fails, fallback to original soup
        pass

    # Reload content and get fresh soup
    content = page.content()
    soup2 = soup  # if parsing fails we keep the earlier one
    try:
        from bs4 import BeautifulSoup
        soup2 = BeautifulSoup(content, "lxml")
    except Exception:
        pass

    # Find job card anchors; inspect Snowflake page: cards are <li class="job-card"> or <a href="/us/en/job/…"> etc.
    for a in soup2.select("a[href*='/us/en/job/'], li.job-card a[href]"):
        link = a.get("href")
        if not link:
            continue
        link = urljoin(base_url, link)
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        # Filter out non-job anchors (e.g., location filters, etc)
        # If link contains '/job/' then likely a real job
        if '/job/' not in link:
            continue
        out.append((link, title))

    return out
