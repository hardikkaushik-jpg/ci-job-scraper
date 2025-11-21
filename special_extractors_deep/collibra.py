import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    out = []
    
    try:
        r = requests.get(GH_URL, headers=headers, timeout=20)
        html = r.text
        
        s = BeautifulSoup(html, "lxml")
        
        for a in s.find_all("a", href=True):
            href = a.get("href")
            if href and "/jobs/" in href:
                text = a.get_text(" ", strip=True)
                if text:
                    out.append((urljoin(GH_URL, href), text))
    
    except Exception as e:
        print("[Collibra extractor error]", e)
    
    return out
