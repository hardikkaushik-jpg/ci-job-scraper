import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra():
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}
    jobs = []

    try:
        # Step 1: Get the main jobs board
        r = requests.get(GH_URL, headers=headers, timeout=20)
        soup = BeautifulSoup(r.text, "lxml")

        # Step 2: Find all job posting links
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if href and "/collibra/jobs/" in href:
                job_url = urljoin(GH_URL, href)

                # Step 3: For each job, fetch job details page
                try:
                    jr = requests.get(job_url, headers=headers, timeout=20)
                    jsoup = BeautifulSoup(jr.text, "lxml")
                    title = jsoup.find("h1", {"class": "app-title"})
                    location = jsoup.find("div", {"class": "location"})
                    description = jsoup.find("div", {"class": "section-wrapper"})

                    jobs.append({
                        "url": job_url,
                        "title": title.get_text(strip=True) if title else "",
                        "location": location.get_text(strip=True) if location else "",
                        "description": description.get_text(" ", strip=True) if description else ""
                    })
                except Exception as je:
                    print("[Job details extractor error]", je)

    except Exception as e:
        print("[Collibra extractor error]", e)

    return jobs

# Usage example:
results = extract_collibra()
for job in results:
    print(job)
