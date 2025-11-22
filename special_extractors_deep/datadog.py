# special_extractors_deep/datadog.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://careers.datadoghq.com"

def extract_jobs(html, url):
    """
    Extract jobs from Datadog filtered or unfiltered lists.
    Extracts: title, url, location, department/team, employment type.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Datadog job cards are in <a role="link"> tags
    cards = soup.select("a[href*='/detail/'][role='link']")

    if not cards:
        return []  # no jobs found (filter empty)

    for c in cards:
        job = {}

        # URL
        href = c.get("href")
        job_url = urljoin(BASE, href)
        job["url"] = job_url

        # Title
        title_el = c.select_one("h3, h4, .JobCard-title, .styles_jobTitle__")
        job["title"] = title_el.get_text(strip=True) if title_el else ""

        # Department / Team
        dept_el = c.select_one("[class*=department], .JobCard-team, .styles_jobTeam__")
        job["team"] = dept_el.get_text(strip=True) if dept_el else ""

        # Location
        loc_el = c.select_one("[class*=location], .JobCard-location, .styles_jobLocation__")
        job["location"] = loc_el.get_text(strip=True) if loc_el else ""

        # Employment type (FT/PT)
        type_el = c.select_one(".JobCard-employment, [class*=employmentType]")
        job["employment_type"] = type_el.get_text(strip=True) if type_el else ""

        # Add to results
        results.append(job)

    return results
