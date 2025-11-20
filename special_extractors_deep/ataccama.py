# ataccama.py
# Special extractor for Ataccama (SmartRecruiters platform)

from bs4 import BeautifulSoup

SMARTRECRUITERS_URL = "https://jobs.smartrecruiters.com/Ataccama/"

def extract_ataccama(soup, page, base_url):
    results = []

    # 1) Directly load SmartRecruiters job board
    try:
        page.goto(SMARTRECRUITERS_URL, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)
        html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(html, "lxml")

    # 2) SmartRecruiters job card selector
    cards = s.select("div.opening > a, a[href*='careers.smartrecruiters.com'], a[href*='/Ataccama/']")
    seen = set()

    for a in cards:
        href = a.get("href")
        if not href:
            continue

        # Normalize link
        if href.startswith("/"):
            link = "https://jobs.smartrecruiters.com" + href
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        # Extract job title
        title = a.get_text(" ", strip=True)
        if not title:
            continue

        # Ignore garbage duplicates
        if "Ataccama" in title and len(title.split()) <= 2:
            continue

        results.append((link, title))

    return results
