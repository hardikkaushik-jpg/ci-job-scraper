# collabira.py — final corrected version
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []
    
    try:
        # Always use requests (CI safe)
        r = requests.get(GH_URL, headers=headers, timeout=20)
        s = BeautifulSoup(r.text, "lxml")

        # Each job is inside a <div class="opening">
        openings = s.select("div.opening")

        for op in openings:
            # LEFT SIDE = real job title (non-clickable)
            title_tag = op.find(text=True, recursive=False)
            if not title_tag or len(title_tag.strip()) < 2:
                continue
            title = title_tag.strip()

            # RIGHT SIDE = the real job link (the "Apply in X" anchor)
            apply_link = op.find("a", href=True)
            if not apply_link:
                continue
            
            href = apply_link.get("href")
            full_link = urljoin(GH_URL, href)
            location_text = apply_link.get_text(" ", strip=True)

            # fetch description
            description = ""
            try:
                d = requests.get(full_link, headers=headers, timeout=20)
                ds = BeautifulSoup(d.text, "lxml")
                content = ds.select_one("div.content")
                if content:
                    description = content.get_text("\n", strip=True)
            except:
                pass

            out.append((full_link, title, description, location_text))

    except Exception as e:
        print("[Collibra extractor error]", e)
        return []
    
    return out
