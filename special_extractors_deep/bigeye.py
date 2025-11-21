import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_bigeye(soup, page, main_url):
    GEM_URL = "https://jobs.gem.com/bigeye?embed=true"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    out = []
    
    try:
        r = requests.get(GEM_URL, headers=headers, timeout=20)
        html = r.text
        
        s = BeautifulSoup(html, "lxml")
        
        for a in s.find_all("a", href=True):
            href = a.get("href")
            if href and "/bigeye/" in href:
                text = a.get_text(" ", strip=True)
                if text:
                    out.append((urljoin(GEM_URL, href), text))
    
    except Exception as e:
        print("[BigEye extractor error]", e)
    
    return out
