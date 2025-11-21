import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []
    seen = set()

    try:
        r = requests.get(GH_URL, headers=headers, timeout=20)
        html = r.text
        s = BeautifulSoup(html, "lxml")

        # Scan ALL anchors (your original logic)
        for a in s.find_all("a", href=True):
            href = a.get("href")
            if not href or "/jobs/" not in href:
                continue

            link = urljoin(GH_URL, href)
            title = a.get_text(" ", strip=True)

            if not title:
                continue

            # --- FILTER OUT THE "Apply in X" ENTRIES ---
            # These were your 61 garbage items
            if title.lower().startswith("apply in "):
                continue

            # avoid duplicates from Greenhouse
            if link in seen:
                continue
            seen.add(link)

            # --- FETCH DETAIL PAGE FOR FULL DESCRIPTION ---
            description = ""
            try:
                detail_req = requests.get(link, headers=headers, timeout=20)
                sd = BeautifulSoup(detail_req.text, "lxml")
                content = sd.select_one("div.content")
                if content:
                    description = content.get_text("\n", strip=True)
            except:
                pass

            # Return EXACTLY what your main pipeline expects
            out.append((link, title, description))

    except Exception as e:
        print("[Collibra extractor error]", e)

    return out
