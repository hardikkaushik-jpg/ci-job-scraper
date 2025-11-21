import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []
    seen = set()

    try:
        # Fetch main job board
        r = requests.get(GH_URL, headers=headers, timeout=20)
        s = BeautifulSoup(r.text, "lxml")

        # Greenhouse job list items live inside div.opening
        for job_item in s.select("div.opening"):

            # Main job link is FIRST anchor inside opening
            title_el = job_item.find("a", href=True)
            if not title_el:
                continue

            title = title_el.get_text(" ", strip=True)
            link = urljoin(GH_URL, title_el.get("href"))

            # ---- FILTER OUT USELESS LINES ----
            if not title or len(title) < 2:
                continue

            # These are the garbage lines we saw earlier
            # "Apply in Raleigh North Carolina"
            # "Apply in Remote"
            if title.lower().startswith("apply in"):
                continue

            if link in seen:
                continue
            seen.add(link)

            # (Optional) fetch job detail description
            description = ""
            try:
                detail_html = requests.get(link, headers=headers, timeout=20).text
                sd = BeautifulSoup(detail_html, "lxml")

                content = sd.select_one("div.content")
                if content:
                    description = content.get_text("\n", strip=True)
            except:
                pass

            # IMPORTANT: return tuples compatible with your pipeline
            out.append((link, title, description))

    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
