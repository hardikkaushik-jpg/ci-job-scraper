import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []

    try:
        # fetch main board
        r = requests.get(GH_URL, headers=headers, timeout=20)
        html = r.text
        # ---- DEBUG: SHOW WHAT HTML GITHUB ACTIONS ACTUALLY RECEIVES ----
        print("\n\n====== DEBUG COLLILBRA HTML START ======")
        print(html[:3000])
        print("\n--- HTML LENGTH:", len(html))
        print("====== DEBUG COLLILBRA HTML END ======\n\n")
        board = BeautifulSoup(r.text, "lxml")

        # Each job row is inside div or li with class "opening"
        openings = board.select("div.opening, li.opening")

        for op in openings:

            # 1. Title: non-clickable text on the left
            title_el = op.select_one(".opening-title")
            if not title_el:
                # fallback: first text node inside op
                title_el = op.find(text=True, recursive=False)

            if not title_el:
                continue

            title = title_el.get_text(strip=True) if hasattr(title_el, "get_text") else title_el.strip()

            # 2. Apply link (the REAL job link)
            apply_a = op.find("a", href=True)
            if not apply_a:
                continue

            job_url = urljoin(GH_URL, apply_a["href"])

            # Output must match your scraper: (link, title)
            out.append((job_url, title))

        return out

    except Exception as e:
        print("[Collibra extractor error]", e)
        return []
