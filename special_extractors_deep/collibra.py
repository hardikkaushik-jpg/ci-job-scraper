from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

def extract_collibra(soup, page, main_url):
    """
    Collibra (Greenhouse) Extractor
    Handles the case where links say 'Apply in Location' but the Title is separate text.
    """
    GH_URL = "https://boards.greenhouse.io/collibra"
    out = []
    
    try:
        print(f"[SPECIAL] Collibra: Navigating to {GH_URL}...")
        # Use the existing Playwright page to fetch the board
        # This avoids needing 'requests' and handles dynamic content if any
        page.goto(GH_URL, wait_until="domcontentloaded", timeout=30000)
        
        # Parse the new page content
        board_soup = BeautifulSoup(page.content(), "lxml")
        
        # Greenhouse standard structure is usually div.opening
        openings = board_soup.select("div.opening")
        
        for op in openings:
            a_tag = op.find("a", href=True)
            if not a_tag:
                continue
                
            link = urljoin(GH_URL, a_tag.get("href"))
            link_text = a_tag.get_text(" ", strip=True)
            
            # Logic: If the link is 'Apply in X', the title is likely the parent text minus the link text
            title = link_text
            
            # Check if this is an "Apply in..." style link
            if re.search(r'^Apply in\b', link_text, re.I):
                # Get all text from the opening block
                full_text = op.get_text(" ", strip=True)
                
                # Remove the link text (location) from the full text to leave just the Title
                # Example: "Senior Data Engineer Apply in Remote" -> "Senior Data Engineer"
                clean_title = full_text.replace(link_text, "").strip()
                
                # Cleanup trailing punctuation or location artifacts
                # e.g., sometimes it leaves a comma or dash
                clean_title = re.sub(r'[,|\-—]$', '', clean_title).strip()
                
                if len(clean_title) > 3:
                    title = clean_title
                else:
                    # Fallback: look for a specific title class if the text extraction failed
                    # Sometimes Greenhouse uses .opening-title or similar
                    header_tag = op.select_one("a[data-mapped='true']") # Standard GH title link often has this
                    if header_tag and header_tag != a_tag:
                        title = header_tag.get_text(strip=True)

            if title and link:
                out.append((link, title))
                
    except Exception as e:
        print(f"[ERROR] Collibra special extractor failed: {e}")
        # Fallback to empty list so main scraper continues
        return []
        
    return out
