# datadog.py
# Deep extractor for Datadog careers page

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_datadog(soup, page, base_url):
    results = []
    seen = set()

    # Datadog jobs always have anchors linking to /job/<slug>
    for a in soup.select("a[href*='/job/']"):
        href = a.get("href", "").strip()
        if not href:
            continue

        link = urljoin(base_url, href)

        if link in seen:
            continue
        seen.add(link)

        # Title extraction: <a> text OR child <h2>
        text = a.get_text(" ", strip=True)
        if not text:
            h2 = a.find("h2")
            if h2:
                text = h2.get_text(" ", strip=True)

        if not text:
            continue

        # Try to find location from nearby elements
        loc = ""

        parent = a.parent
        if parent:
            # Several Datadog builds place location in sibling spans
            possible = parent.find_all(["span", "div"])
            for el in possible:
                t = el.get_text(" ", strip=True)
                if (
                    t
                    and any(k in t.lower() for k in [
                        "remote", "germany", "usa", "france",
                        "new york", "london", "singapore",
                        "india", "poland", "canada"
                    ])
                ):
                    loc = t
                    break

        # fallback: look for data-qa='location'
        if not loc:
            loc_el = soup.find("span", {"data-qa": "location"})
            if loc_el:
                loc = loc_el.get_text(" ", strip=True)

        label = f"{text} ({loc})" if loc else text

        results.append((link, label))

    return results
