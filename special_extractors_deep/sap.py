# sap.py
# Deep extractor for SAP (SuccessFactors ATS)
#
# This is *not* generic — custom-tailored to SAP's SuccessFactors HTML.

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_sap(soup, page, base_url):
    results = []
    seen = set()

    # SAP job anchors typically contain '/job/' OR have class 'jobTitle-link'
    selectors = [
        "a.jobTitle-link",
        "a[href*='/job/']",
        "a.sf-job-list-title",  # rare but appears in some markets
    ]

    anchors = []
    for sel in selectors:
        anchors.extend(soup.select(sel))

    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # Extract title (anchor text or nested title div)
        title = a.get_text(" ", strip=True)
        if not title:
            h = a.find("h2") or a.find("h3")
            if h:
                title = h.get_text(" ", strip=True)

        if not title:
            continue

        # Try extracting location — SAP uses multiple patterns:
        loc = ""

        # 1) Look in parent tile
        tile = a.find_parent(["div", "article", "li"])
        if tile:
            # Possible patterns inside SAP tiles
            loc_candidates = tile.select(".jobLocation, .job-location, .jLR, span[class*='location']")
            for el in loc_candidates:
                txt = el.get_text(" ", strip=True)
                if txt:
                    loc = txt
                    break

        # 2) If missing, try looking at siblings
        if not loc and tile:
            for sib in tile.find_all(["div", "span"], recursive=True):
                txt = sib.get_text(" ", strip=True)
                if txt and any(
                    k in txt.lower()
                    for k in [
                        "remote",
                        "hybrid",
                        "germany",
                        "india",
                        "usa",
                        "canada",
                        "france",
                        "australia",
                        "united",
                        "singapore",
                        "japan",
                        "brazil",
                    ]
                ):
                    loc = txt
                    break

        # Combine title + location as label
        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
