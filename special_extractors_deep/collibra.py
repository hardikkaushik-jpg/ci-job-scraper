import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []
    seen = set()

    try:
        # STEP 1 — load main board
        r = requests.get(GH_URL, headers=headers, timeout=20)
        s = BeautifulSoup(r.text, "lxml")

        # Each job is inside <div class="opening">
        for job_item in s.select("div.opening"):
            title_el = job_item.select_one("a[href*='/jobs/']")
            if not title_el:
                continue

            title = title_el.get_text(" ", strip=True)
            link = urljoin(GH_URL, title_el.get("href"))

            # Skip “Apply in X”
            if title.lower().startswith("apply in"):
                continue

            if link in seen:
                continue
            seen.add(link)

            # STEP 2 — fetch job detail page
            description = ""
            try:
                detail_html = requests.get(link, headers=headers, timeout=20).text
                sd = BeautifulSoup(detail_html, "lxml")

                # Collibra puts job text inside <div class="content">
                content_div = sd.select_one("div.content")
                if content_div:
                    description = content_div.get_text("\n", strip=True)

            except Exception:
                pass

            # return (link, title, description)
            out.append((link, title, description))

    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
