# montecarlo.py
# Deep extractor for Monte Carlo (AshbyHQ)

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_monte_carlo(soup, page, base_url):
    results = []
    seen = set()

    # Ashby layouts always provide anchors with '/job/' or company slug
    for a in soup.select("a[href*='/job/'], a[href*='montecarlodata']"):
        href = a.get("href", "").strip()
        if not href:
            continue

        link = urljoin(base_url, href)
        if link in seen:
            continue
        seen.add(link)

        # Extract text/title (Ashby often uses <h3>)
        title = (
            a.get_text(" ", strip=True)
            or (a.find("h3").get_text(" ", strip=True) if a.find("h3") else "")
        )

        if not title:
            continue

        # Try to detect a nearby location
        loc = ""

        # Look in siblings/parents for useful text
        container = a.parent
        if container:
            for el in container.find_all(["div", "span"], recursive=False):
                txt = el.get_text(" ", strip=True)
                if txt and any(
                    k in txt.lower()
                    for k in [
                        "remote",
                        "hybrid",
                        "united",
                        "usa",
                        "europe",
                        "germany",
                        "india",
                        "london",
                        "new york",
                        "san francisco",
                        "belgium",
                        "canada",
                    ]
                ):
                    loc = txt
                    break

        label = f"{title} ({loc})" if loc else title

        results.append((link, label))

    return results
