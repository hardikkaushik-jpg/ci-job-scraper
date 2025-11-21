# collabira.py — final stable version

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    out = []
    seen = set()

    try:
        print("[SPECIAL] Collibra: Fetching Greenhouse board via requests...")
        
        # Use requests — NOT Playwright (Greenhouse blocks chromedriver in CI)
        r = requests.get(GH_URL, headers=headers, timeout=20)
        s = BeautifulSoup(r.text, "lxml")
        
        # This is the working selector in your pipeline
        anchors = s.find_all("a", href=True)
        
        for a in anchors:
            href = a.get("href")
            if not href or "/jobs/" not in href:
                continue
            
            link = urljoin(GH_URL, href)
            text = a.get_text(" ", strip=True)
            
            if not text:
                continue
            
            # FILTER: Remove useless "Apply in X"
            if re.match(r"^Apply in\b", text, re.I):
                continue
            
            if link in seen:
                continue
            seen.add(link)
            
            # OPTIONAL: fetch job description
            description = ""
            try:
                detail = requests.get(link, headers=headers, timeout=20)
                dsoup = BeautifulSoup(detail.text, "lxml")
                content = dsoup.select_one("div.content")
                if content:
                    description = content.get_text("\n", strip=True)
            except:
                pass
            
            out.append((link, text, description))
    
    except Exception as e:
        print(f"[ERROR] Collibra extractor failed: {e}")
        return []

    return out
