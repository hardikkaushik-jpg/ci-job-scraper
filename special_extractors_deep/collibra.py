import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = { "User-Agent": "Mozilla/5.0" }

    out = []

    try:
        r = requests.get(GH_URL, headers=headers, timeout=20)
        html = r.text
        s = BeautifulSoup(html, "lxml")

        # ------------------------------------------
        # PATCHED LOGIC: Loop job containers instead of raw anchors
        # ------------------------------------------
        for op in s.select("div.opening"):

            # LEFT SIDE TITLE (non-clickable)
            title_tag = op.find(text=True, recursive=False)
            if not title_tag:
                continue
            title = title_tag.strip()

            # RIGHT SIDE APPLY LINK (actual job URL)
            apply_a = op.find("a", href=True)
            if not apply_a:
                continue

            link = urljoin(GH_URL, apply_a["href"])
            location_text = apply_a.get_text(" ", strip=True)

            # Fetch description from the job detail page
            description = ""
            try:
                detail_html = requests.get(link, headers=headers, timeout=20).text
                detail_soup = BeautifulSoup(detail_html, "lxml")
                content = detail_soup.select_one("div.content")
                if content:
                    description = content.get_text("\n", strip=True)
            except:
                pass

            # Return correctly combined tuple
            out.append((link, title, description, location_text))

        return out

    except Exception as e:
        print("[Collibra extractor error]", e)
        return []
