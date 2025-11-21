import requests

def extract_bigeye(soup, page, main_url):
    API_URL = "https://api.gem.com/public/boards/bigeye"
    headers = {"User-Agent": "Mozilla/5.0"}

    out = []

    try:
        data = requests.get(API_URL, headers=headers, timeout=20).json()

        for job in data.get("jobs", []):
            title = job.get("text")
            link = job.get("url")
            if not title or not link:
                continue

            # Drop garbage / non-clickable / drafts
            if "apply" in title.lower():
                continue

            out.append((link, title))

    except Exception as e:
        print("[BigEye API extractor error]", e)

    return out
