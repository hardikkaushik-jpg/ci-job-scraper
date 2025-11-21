import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []

    try:
        # Fetch main greenhouse page
        r = requests.get(GH_URL, headers=headers, timeout=20)
        board = BeautifulSoup(r.text, "lxml")

        # Get all job links (Apply in X)
        links = board.find_all("a", href=True)

        for a in links:
            href = a.get("href")

            # Only catch real job pages
            if not href or "/jobs/" not in href:
                continue

            job_url = urljoin(GH_URL, href)

            # GO INSIDE THE JOB PAGE
            try:
                jr = requests.get(job_url, headers=headers, timeout=20)
                jsoup = BeautifulSoup(jr.text, "lxml")

                # real title
                title_el = jsoup.select_one("h1.app-title")
                title = title_el.get_text(strip=True) if title_el else ""

                # real location
                loc_el = jsoup.select_one("div.location")
                location = loc_el.get_text(strip=True) if loc_el else ""

                # full description (used later for relevance score)
                desc_el = jsoup.select_one("div.section-wrapper")
                description = desc_el.get_text(" ", strip=True) if desc_el else ""

                if title:
                    out.append((job_url, title))  # this matches your scraper format

            except Exception as je:
                print("[Collibra job detail error]", je)

    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
