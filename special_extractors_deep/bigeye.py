import requests
from bs4 import BeautifulSoup

def extract_bigeye(soup, page, main_url):
    API = "https://jobs.gem.com/companies/bigeye?embed=true"
    headers = {"User-Agent": "Mozilla/5.0"}
    out = []

    try:
        r = requests.get(API, headers=headers, timeout=20)
        html = r.text

        s = BeautifulSoup(html, "lxml")

        # Each job is inside: div[data-qa="job-listing"]
        jobs = s.select("div[data-qa='job-listing']")

        for job in jobs:
            title_el = job.select_one("[data-qa='job-title']")
            loc_el = job.select_one("[data-qa='job-location']")
            link_el = job.find("a", href=True)

            if not link_el:
                continue

            title = title_el.get_text(strip=True) if title_el else ""
            loc = loc_el.get_text(strip=True) if loc_el else ""
            link = link_el["href"]

            # Make absolute URL
            if link.startswith("/"):
                link = "https://jobs.gem.com" + link

            out.append((link, title + " | " + loc))

    except Exception as e:
        print("[BigEye extractor error]", e)

    return out
