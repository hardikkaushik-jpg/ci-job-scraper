import requests

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
        loc = ""
        if job.get("location"):
            loc = job["location"].get("name", "")

        # Return same shape as other extractors
        out.append((link, title, None))

    print(f"[Databricks API] Extracted {len(out)} jobs")
    return out


SPECIAL_EXTRACTORS_DEEP["Databricks"] = extract_databricks
