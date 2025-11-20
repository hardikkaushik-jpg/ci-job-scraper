# extractor_cloudera.py (FULLY FIXED — MATCHES WORKDAY DOM)

from urllib.parse import urljoin

def extract_cloudera(soup, page, base_url):
    """
    Cloudera uses Workday. We only extract job-card anchors,
    no detail fetch. Return (href, card_text, element) exactly
    like the Fivetran extractor pattern.
    """
    out = []

    # Workday job title anchors
    anchors = soup.select("a[data-automation-id='jobTitle']")
    for a in anchors:
        href = a.get("href")
        text = a.get_text(" ", strip=True)

        if not href or not text:
            continue

        # make absolute link
        link = urljoin(base_url, href)

        # return full Fivetran-style tuple:
        # (link, card_text, the actual element)
        out.append((link, text, a))

    return out
