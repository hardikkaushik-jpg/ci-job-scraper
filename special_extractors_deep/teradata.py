# teradata.py
# Deep extractor for Teradata (Workday-based)
# URL: https://careers.teradata.com/jobs

import json, re, time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

RELEVANT = re.compile(
    r"(data|etl|snowflake|pipeline|integration|platform|cloud|engineer|analytics|bi)",
    re.I
)

def fetch_detail(page, link):
    """Fetch full job detail page safely."""
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(1000)

        s = BeautifulSoup(page.content(), "lxml")
        title = ""
        loc = ""
        date = ""

        h = s.find("h1")
        if h:
            title = h.get_text(" ", strip=True)

        # Workday location selectors
        for sel in [".location", ".job-location", "[data-automation-id='location']"]:
            el = s.select_one(sel)
            if el:
                loc = el.get_text(" ", strip=True)
                break

        # JSON-LD for date + location
        for sc in s.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string or "{}")
            except:
                continue

            if isinstance(data, dict):
                if "datePosted" in data:
                    date_raw = data["datePosted"]
                    if "T" in date_raw:
                        date = date_raw.split("T")[0]

                jl = data.get("jobLocation")
                if isinstance(jl, dict) and not loc:
                    addr = jl.get("address", {})
                    city = addr.get("addressLocality", "")
                    region = addr.get("addressRegion", "")
                    country = addr.get("addressCountry", "")
                    loc = ", ".join([x for x in [city, region, country] if x])

        return title, loc, date

    except Exception:
        return "", "", ""


def extract_teradata(soup, page, base_url):
    """Main Teradata extractor – Workday JSON parsing."""
    out = []

    # Step 1: Find Workday job JSON blob
    scripts = soup.find_all("script", {"type": "application/json"})
    json_blob = None

    for sc in scripts:
        txt = sc.string
        if txt and '"jobFamilyGroup"' in txt:
            json_blob = txt.strip()
            break

    if not json_blob:
        return out

    try:
        data = json.loads(json_blob)
    except Exception:
        return out

    # Workday stores jobs here:
    jobs = data.get("jobPostings", []) or data.get("children", [])

    for j in jobs:
        # Workday structure differs between deployments
        title = j.get("title") or j.get("displayName") or ""
        link = j.get("externalPath") or j.get("externalUrl") or j.get("canonicalPositionUrl") or ""
        loc = j.get("location") or j.get("city") or ""
        date = j.get("postedOn") or ""

        if not title or not link:
            continue

        full_link = urljoin(base_url, link)

        # relevance filter
        combined_text = f"{title} {loc}"
        if not RELEVANT.search(combined_text):
            continue

        # fetch detail page for clean metadata
        title2, loc2, date2 = fetch_detail(page, full_link)

        final_title = title2 or title
        final_loc = loc2 or loc
        final_date = date2 or date

        label = f"{final_title} ({final_loc})" if final_loc else final_title

        out.append((full_link, label))

    return out
