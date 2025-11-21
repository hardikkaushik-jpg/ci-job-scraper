import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    API_URL = "https://boards.greenhouse.io/v1/boards/collibra/jobs"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []

    try:
        r = requests.get(API_URL, headers=headers, timeout=20)
        data = r.json()

        for job in data.get("jobs", []):
            title = job.get("title", "").strip()
            job_url = job.get("absolute_url", "").strip()

            if title and job_url:
                out.append((job_url, title))

    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
