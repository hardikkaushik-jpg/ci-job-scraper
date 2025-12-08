import requests
from bs4 import BeautifulSoup

def extract_databricks(soup, page, main_url):
    api = "https://boards-api.greenhouse.io/v1/boards/databricks/jobs?content=true"

    try:
        r = requests.get(api, timeout=15)
        data = r.json()
    except Exception as e:
        print(f"[Databricks API ERROR] {e}")
        return []

    out = []
    for job in data.get("jobs", []):
        link = job.get("absolute_url", "")
        title = job.get("title", "")

        # Extract clean location
        loc = ""
        if job.get("location") and job["location"].get("name"):
            loc = job["location"]["name"]

        # Extract posting date
        posting_date = ""
        if "updated_at" in job:
            posting_date = job["updated_at"].split("T")[0]

        # Extract full description text
        desc_html = job.get("content", "")
        desc_text = ""
        if desc_html:
            soup_desc = BeautifulSoup(desc_html, "lxml")
            desc_text = soup_desc.get_text(" ", strip=True)[:5000]

        # Pass enriched data into pipeline
        out.append((link, title, desc_text, loc, posting_date))

    print(f"[Databricks API] Extracted {len(out)} jobs with full metadata")
    return out
