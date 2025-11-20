# influxdata.py
# Deep extractor for InfluxData
# URL: https://www.influxdata.com/careers/#jobs

import re, time, json
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Strong relevance filter
RELEVANT = re.compile(
    r"(data|cloud|platform|engineer|etl|integration|pipeline|backend|golang|distributed)",
    re.I
)

def fetch_detail(page, link):
    """Optional detail extraction for location + date."""
    try:
        page.goto(link, timeout=45000, wait_until="networkidle")
        page.wait_for_timeout(900)

        s = BeautifulSoup(page.content(), "lxml")

        title = ""
        loc = ""
        date = ""

        # generic title selector (non-Workday)
        h = s.find("h1")
        if h:
            title = h.get_text(" ", strip=True)

        # location selectors (Workable/Greenhouse style)
        for sel in [
            ".location", ".job-location", ".posting-location",
            "[data-qa='job-location']", "[data-test='job-location']"
        ]:
            el = s.select_one(sel)
            if el:
                loc = el.get_text(" ", strip=True)
                break

        # JSON-LD
        for script in s.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "{}")
            except:
                continue

            if isinstance(data, dict):
                if "datePosted" in data:
                    date_raw = data["datePosted"]
                    if "T" in date_raw:
                        date = date_raw.split("T")[0]

        return title, loc, date

    except Exception:
        return "", "", ""


def extract_influxdata(soup, page, base_url):
    """Main extractor for InfluxData jobs."""
    out = []

    # Their job cards are inside a section with CSS grid/list layout
    job_cards = soup.select("div.career-job, li.career-job, div.job, li.job")

    # fallback: scan anchors (Influx sometimes changes markup)
    if not job_cards:
        job_cards = soup.select("a[href*='/careers/']")

    for card in job_cards:
        a = card.find("a", href=True)
        if not a:
            continue

        link = urljoin(base_url, a.get("href"))
        text = a.get_text(" ", strip=True)

        # Extract title + location from text
        title = text
        loc = ""

        # Look for patterns like "Software Engineer – Remote" or "| USA"
        if " - " in text:
            parts = text.split(" - ")
            title = parts[0].strip()
            loc = parts[-1].strip()
        elif "|" in text:
            parts = text.split("|")
            title = parts[0].strip()
            loc = parts[-1].strip()

        # Relevance filtering
        combined = f"{title} {loc}"
        if not RELEVANT.search(combined):
            continue

        # Fetch detail for better metadata
        t2, l2, d2 = fetch_detail(page, link)

        final_title = t2 or title
        final_loc = l2 or loc
        label = f"{final_title} ({final_loc})" if final_loc else final_title

        out.append((link, label))

    return out
