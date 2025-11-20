# alteryx.py
# Deep extractor for Alteryx (Workday ATS)

from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin

def extract_alteryx(soup, page, base_url):
    results = []

    # Workday pages always embed job data in:
    #  <script type="application/ld+json"> ... JobPosting ... </script>

    scripts = soup.find_all("script", type="application/ld+json")
    for s in scripts:
        raw = s.string or ""
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except:
            continue

        # Could be an array or single object
        items = data if isinstance(data, list) else [data]

        for obj in items:
            if not isinstance(obj, dict):
                continue

            # Ensure it's a JobPosting type
            if obj.get("@type") not in ("JobPosting", "JobPostingType", "Posting"):
                continue

            title = obj.get("title") or ""
            link  = obj.get("url") or ""

            if not title or not link:
                continue

            # Normalize link
            link = urljoin(base_url, link)

            # Extract location if present
            loc = ""
            jl = obj.get("jobLocation") or obj.get("jobLocations")
            if jl:
                entry = jl[0] if isinstance(jl, list) else jl
                if isinstance(entry, dict):
                    addr = entry.get("address") or entry
                    if isinstance(addr, dict):
                        parts = []
                        for k in ("addressLocality", "addressRegion", "addressCountry"):
                            if addr.get(k):
                                parts.append(addr[k])
                        if parts:
                            loc = ", ".join(parts)

            # Push result: (link, title + optional location)
            label = f"{title} ({loc})" if loc else title
            results.append((link, label))

    return results
