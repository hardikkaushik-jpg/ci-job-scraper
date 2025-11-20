# extractor_cloudera.py — FINAL WORKING VERSION

from urllib.parse import urljoin
from bs4 import BeautifulSoup

def extract_cloudera(soup, page, base_url):
    """
    Proper Workday extractor.
    Force hydration by waiting for job cards.
    Returns Fivetran-style tuples: (href, text, element).
    """
    out = []

    try:
        # force hydration — this is CRITICAL
        page.wait_for_selector("a[data-automation-id='jobTitle']", timeout=60000)
        html = page.content()
        s = BeautifulSoup(html, "lxml")

        for a in s.select("a[data-automation-id='jobTitle']"):
            href = a.get("href")
            text = a.get_text(" ", strip=True)

            if not href or not text:
                continue

            link = urljoin(base_url, href)
            out.append((link, text, a))

    except Exception as e:
        print("[CLOUDERA-EXTRACTOR] Failed:", e)

    return out
