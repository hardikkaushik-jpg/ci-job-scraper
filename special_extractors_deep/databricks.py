# special_extractors_deep/databricks.py — v2.0
# Uses Greenhouse API with content=true for full description
# Fixed: uses first_published_at (actual posting date) not updated_at (edit date)

import requests
import re
from bs4 import BeautifulSoup

# Roles to drop immediately — pure sales/legal/HR with no CI signal
DATABRICKS_DROP = re.compile(
    r'\b(account executive|business development rep|bdr|sdr|legal counsel|'
    r'executive assistant|talent sourcer|recruiter|paralegal|'
    r'deployment strategist|proposal coordinator|field cto|'
    r'regional vice president|rvp)\b',
    re.I
)

def extract_databricks(soup, page, main_url):
    api = "https://boards-api.greenhouse.io/v1/boards/databricks/jobs?content=true"

    try:
        r = requests.get(api, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Databricks API ERROR] {e}")
        return []

    out = []
    for job in data.get("jobs", []):
        link  = job.get("absolute_url", "")
        title = (job.get("title") or "").strip()

        if not link or not title:
            continue

        # Drop pure noise roles at extraction time
        if DATABRICKS_DROP.search(title):
            continue

        # Location
        loc = ""
        loc_data = job.get("location")
        if loc_data and isinstance(loc_data, dict):
            loc = loc_data.get("name", "")

        # Use first_published_at (actual public posting date)
        # Fall back to updated_at only if unavailable
        posting_date = ""
        for date_field in ("first_published_at", "updated_at"):
            raw_date = job.get(date_field, "")
            if raw_date:
                posting_date = raw_date.split("T")[0]
                break

        # Description text from HTML content
        desc_html = job.get("content", "")
        desc_text = ""
        if desc_html:
            try:
                soup_desc = BeautifulSoup(desc_html, "lxml")
                desc_text = soup_desc.get_text(" ", strip=True)[:4000]
            except Exception:
                pass

        # 5-tuple: (link, title, description, location, posting_date)
        out.append((link, title, desc_text, loc, posting_date))

    print(f"[Databricks API] Extracted {len(out)} jobs")
    return out
