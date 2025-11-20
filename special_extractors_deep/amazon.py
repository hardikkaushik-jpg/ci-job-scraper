# amazon.py
# Deep extractor for https://www.amazon.jobs/en/
# Extracts all AWS + Amazon Data/ETL/Engineering relevant roles via API calls.

import json, re, time
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup

RELEVANT = re.compile(
    r"(data|etl|pipeline|integration|analytics|warehouse|lake|engineer|ml|"
    r"sde|big\s*data|bi|platform|cloud|aws)",
    re.I
)

API_BASE = "https://www.amazon.jobs/en/search.json?"


def extract_amazon(soup, page, base_url):
    results = []
    seen = set()

    # Amazon search API supports pagination & filters
    # We fetch first ~10 pages (enough for 500–800 roles)
    MAX_PAGES = 10

    for page_num in range(1, MAX_PAGES + 1):

        params = {
            "normalized_country_code[]": "",
            "radius": "24km",
            "offset": (page_num - 1) * 10,
            "result_limit": 10,
            "sort": "recent",
            "category[]": "Software Development",
            "category[]": "Business Intelligence",
            "category[]": "Machine Learning Science",
            "category[]": "Solutions Architect",
        }

        api_url = API_BASE + urlencode(params, doseq=True)

        try:
            page.goto(api_url, wait_until="networkidle", timeout=45000)
            time.sleep(0.3)
            raw = page.inner_text("pre")  # API result is JSON in <pre>
            data = json.loads(raw)
        except Exception:
            continue

        jobs = data.get("jobs", [])
        if not jobs:
            break

        for j in jobs:
            title = j.get("title", "").strip()
            link = j.get("job_path", "")
            loc = j.get("normalized_location", "")
            date = j.get("posted_date", "")

            if not title or not link:
                continue

            full_link = urljoin("https://www.amazon.jobs", link)

            # Check data relevance
            if not RELEVANT.search(title):
                continue

            if full_link in seen:
                continue
            seen.add(full_link)

            label = f"{title} ({loc})" if loc else title

            results.append((full_link, label))

    return results
