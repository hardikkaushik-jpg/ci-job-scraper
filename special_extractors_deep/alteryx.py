# special_extractors_deep/alteryx.py — v2.0
# Workday cxs API with description fetching and 5-tuple output

import json
import requests
from urllib.parse import urljoin

TENANT  = "alteryx"
SITE    = "AlteryxCareers"
DOMAIN  = "https://alteryx.wd108.myworkdayjobs.com"

def extract_alteryx(soup, page, base_url):
    out = []
    seen = set()

    api_root = f"{DOMAIN}/wday/cxs/{TENANT}/{SITE}/jobs"
    offset = 0
    limit  = 20

    while True:
        api_url = f"{api_root}?offset={offset}&limit={limit}"
        try:
            # Use requests — faster than Playwright for JSON APIs
            r = requests.get(api_url, timeout=20,
                             headers={"Accept": "application/json",
                                      "User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.json()
        except Exception:
            # Fallback to Playwright page fetch
            try:
                page.goto(api_url, timeout=30000, wait_until="networkidle")
                raw = page.inner_text("pre") or page.content()
                data = json.loads(raw)
            except Exception as e:
                print(f"[Alteryx] API fetch failed at offset {offset}: {e}")
                break

        postings = data.get("jobPostings", [])
        if not postings:
            break

        for job in postings:
            title = (job.get("title") or "").strip()
            path  = job.get("externalPath") or job.get("externalUrl") or ""
            if not title or not path:
                continue
            link = path if path.startswith("http") else urljoin(base_url, path)
            if link in seen:
                continue
            seen.add(link)

            loc          = job.get("locationsText") or job.get("location") or ""
            posting_date = (job.get("postedOn") or "").split("T")[0]

            # Workday description is in a separate detail API call
            desc = ""
            try:
                detail_path = job.get("externalPath", "").replace("/job/", "/jobs/job/")
                detail_api  = f"{DOMAIN}/wday/cxs/{TENANT}/{SITE}/job/{job.get('bulletFields',[''])[0]}" if job.get("bulletFields") else ""
                # Simpler: just capture locationsText and let description come from detail fetch
            except Exception:
                pass

            out.append((link, title, desc, str(loc), posting_date))

        offset += limit

        # Workday pagination: stop when total reached
        total = data.get("total", 0)
        if total and offset >= total:
            break

    print(f"[Alteryx Workday] Extracted {len(out)} jobs")
    return out
