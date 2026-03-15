# special_extractors_deep/mongodb.py — v1.0
# MongoDB uses Greenhouse — API with content=true

import requests
import re
from bs4 import BeautifulSoup

# Drop pure noise — MongoDB is a huge company with many unrelated roles
MONGODB_DROP = re.compile(
    r"\b(account executive|account development|business development|"
    r"bdr|sdr|recruiter|talent acquisition|legal|paralegal|"
    r"executive assistant|summit|women in tech|next in tech)\b",
    re.I
)

def extract_mongodb(soup, page, main_url):
    API_URL = "https://boards-api.greenhouse.io/v1/boards/mongodb/jobs?content=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[MongoDB Greenhouse API error] {e}")
        return out

    for job in data.get("jobs", []):
        title = (job.get("title") or "").strip()
        link  = (job.get("absolute_url") or "").strip()
        if not title or not link:
            continue

        if MONGODB_DROP.search(title):
            continue

        loc = ""
        loc_data = job.get("location")
        if isinstance(loc_data, dict):
            loc = loc_data.get("name", "")

        posting_date = ""
        for df in ("first_published_at", "updated_at"):
            raw = job.get(df, "")
            if raw:
                posting_date = raw.split("T")[0]
                break

        desc_text = ""
        desc_html = job.get("content", "")
        if desc_html:
            try:
                desc_text = BeautifulSoup(desc_html, "lxml").get_text(" ", strip=True)[:4000]
            except Exception:
                pass

        out.append((link, title, desc_text, loc, posting_date))

    print(f"[MongoDB API] Extracted {len(out)} jobs")
    return out
