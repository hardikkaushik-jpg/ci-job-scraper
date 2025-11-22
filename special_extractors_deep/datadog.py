import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

CATEGORY_URLS = {
    "Engineering": "https://careers.datadoghq.com/all-jobs/?parent_department_Engineering[0]=Engineering",
    "Technical Solutions": "https://careers.datadoghq.com/all-jobs/?parent_department_TechnicalSolutions[0]=Technical%20Solutions",
    "Product Management": "https://careers.datadoghq.com/all-jobs/?parent_department_ProductManagement[0]=Product%20Management",
}

def extract_datadog(soup, page, main_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    jobs = []

    for team, url in CATEGORY_URLS.items():
        try:
            r = requests.get(url, headers=headers, timeout=20)
            html = r.text
            s = BeautifulSoup(html, "lxml")

            # Job cards
            cards = s.select("a[href*='/job/']")
            for c in cards:
                href = c.get("href")
                if not href: 
                    continue

                title = c.get_text(" ", strip=True)
                if not title:
                    continue

                full_url = urljoin("https://careers.datadoghq.com", href)
                jobs.append((full_url, f"{title} ({team})"))

        except Exception as e:
            print("[Datadog extractor error]", e)

    return jobs
