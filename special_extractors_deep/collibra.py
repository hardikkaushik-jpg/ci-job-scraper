import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time

def get_gh_details(job_url, headers):
    """
    Visits the specific job page to extract the real H1 title 
    and location, avoiding the generic 'Apply in...' text.
    """
    try:
        # Be polite with a tiny sleep to avoid hitting rate limits on 60+ requests
        time.sleep(0.2) 
        r = requests.get(job_url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None, None
            
        s = BeautifulSoup(r.text, "lxml")
        
        # Greenhouse job pages usually have the title in an <h1 class="app-title">
        title_tag = s.find("h1", class_="app-title")
        
        # Fallback if class is missing, just find first H1
        if not title_tag:
            title_tag = s.find("h1")
            
        real_title = title_tag.get_text(" ", strip=True) if title_tag else None
        
        # Optional: Try to grab location from the details page too
        loc_tag = s.find("div", class_="location")
        real_location = loc_tag.get_text(" ", strip=True) if loc_tag else "Remote"
        
        return real_title, real_location

    except Exception as e:
        # print(f"[Deep Scrape Error] {job_url}: {e}") # Uncomment for debug
        return None, None

def extract_collibra(soup, page, main_url):
    GH_URL = "https://boards.greenhouse.io/collibra"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    out = []
    
    try:
        r = requests.get(GH_URL, headers=headers, timeout=20)
        html = r.text
        s = BeautifulSoup(html, "lxml")
        
        processed_urls = set()

        for a in s.find_all("a", href=True):
            href = a.get("href")
            
            # Filter for actual job links
            if href and "/jobs/" in href:
                full_url = urljoin(GH_URL, href)
                
                # Deduplication check (Greenhouse sometimes links the same job twice)
                if full_url in processed_urls:
                    continue
                processed_urls.add(full_url)

                # --- THE FIX: DEEP SCRAPING ---
                # Instead of using the text from the main page (which is "Apply in..."),
                # we visit the link to get the real H1 title.
                real_title, real_loc = get_gh_details(full_url, headers)
                
                if real_title:
                    # We append the URL and the Real Title. 
                    # Note: If your system expects location in the tuple, add it here. 
                    # Current standard seems to be (Link, Title) based on your previous code.
                    out.append((full_url, real_title))
                    print(f"[DEBUG] Found: {real_title} @ {full_url}")
                else:
                    # Fallback: If deep scrape fails, ignore or keep original (dropping avoids bad data)
                    pass
    
    except Exception as e:
        print("[Collibra extractor error]", e)
    
    return out
