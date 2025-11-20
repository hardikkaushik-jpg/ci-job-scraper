# atlan.py
# Special extractor for Atlan (AshbyHQ platform)

from bs4 import BeautifulSoup

def extract_atlan(soup, page, base_url):
    results = []

    # 1) Ashby always embeds job board URL: https://jobs.ashbyhq.com/atlan
    ashby_url = "https://jobs.ashbyhq.com/atlan"

    try:
        page.goto(ashby_url, timeout=45000, wait_until="networkidle")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)
        ashby_html = page.content()
    except Exception:
        return results

    s = BeautifulSoup(ashby_html, "lxml")

    # 2) Ashby job cards follow this selector
    cards = s.select("a[href*='/jobs/']")
    seen = set()

    for a in cards:
        href = a.get("href")
        if not href:
            continue
        if "/jobs/" not in href:
            continue

        # Normalise link
        if href.startswith("/"):
            link = "https://jobs.ashbyhq.com" + href
        else:
            link = href

        if link in seen:
            continue
        seen.add(link)

        title = a.get_text(" ", strip=True)
        if not title:
            continue

        results.append((link, title))

    return results
