# special_extractors_deep/zilliz.py — v1.0
# Zilliz (Milvus parent company) uses Lever API
# Also has some roles on Greenhouse — both covered

import requests
from bs4 import BeautifulSoup
from datetime import datetime

def extract_zilliz(soup, page, main_url):
    out = []

    # Primary: Lever API
    lever_out = _lever(out)

    # Secondary: Greenhouse API
    gh_out = _greenhouse()

    combined = lever_out + gh_out

    # Dedup by link
    seen = set()
    final = []
    for item in combined:
        link = item[0]
        if link not in seen:
            seen.add(link)
            final.append(item)

    print(f"[Zilliz/Milvus] Extracted {len(final)} jobs (Lever + Greenhouse)")
    return final


def _lever(out):
    API_URL = "https://api.lever.co/v0/postings/zilliz?mode=json"
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        jobs = r.json()
    except Exception as e:
        print(f"[Zilliz Lever API error] {e}")
        return results

    for job in jobs:
        title = (job.get("text") or "").strip()
        link  = (job.get("hostedUrl") or "").strip()
        if not title or not link:
            continue

        loc = job.get("categories", {}).get("location") or ""
        posting_date = ""
        ts = job.get("createdAt")
        if ts:
            try:
                posting_date = datetime.fromtimestamp(ts / 1000).date().isoformat()
            except Exception:
                pass

        desc = ""
        desc_html = job.get("descriptionPlain") or job.get("description") or ""
        if desc_html:
            try:
                desc = BeautifulSoup(desc_html, "lxml").get_text(" ", strip=True)[:4000]
            except Exception:
                desc = desc_html[:4000]

        results.append((link, title, desc, str(loc), posting_date))

    return results


def _greenhouse():
    API_URL = "https://boards-api.greenhouse.io/v1/boards/zilliz/jobs?content=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[Zilliz Greenhouse API error] {e}")
        return results

    for job in data.get("jobs", []):
        title = (job.get("title") or "").strip()
        link  = (job.get("absolute_url") or "").strip()
        if not title or not link:
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

        results.append((link, title, desc_text, loc, posting_date))

    return results
