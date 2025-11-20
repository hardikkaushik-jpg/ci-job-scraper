# special_extractors_deep.py
# Deep, per-site extractors that follow the Fivetran-style contract:
# Each extractor(soup, page, base_url) -> iterable of (href, title, element_like)
# Designed to be imported as:
#   from special_extractors_deep import SPECIAL_EXTRACTORS_DEEP

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re, json

def normalize_link(base, href):
    if not href: return ""
    href = href.strip()
    if href.startswith("//"): href = "https:" + href
    parsed = urlparse(href)
    if parsed.netloc: return href
    try:
        return urljoin(base, href)
    except:
        return href

# small helper to add unique results
def _add(out, link, text, el=None):
    if not link: return
    out.append((link, text or "", el))

# ------------------ Individual extractors ------------------
# The goal: return high-precision job anchors for that company.
# Keep logic conservative (prefer ATS links, anchored job cards, JSON-LD).

def _extract_jsonld_jobs(soup, base_url):
    out = []
    for script in soup.find_all("script", type="application/ld+json"):
        txt = script.string or ""
        if not txt: continue
        try:
            payload = json.loads(txt)
        except:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            if isinstance(it, dict) and it.get("title") and it.get("url"):
                _add(out, normalize_link(base_url, it.get("url")), it.get("title"), script)
    return out

# Generic helper anchored extractor tuned to keywords
def _generic_anchor_kw(soup, base_url, keywords):
    out = []
    kws = re.compile(keywords, re.I)
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        text = a.get_text(" ", strip=True) or ""
        if any(x in (href or "").lower() for x in ("greenhouse","lever","myworkday","ashby","bamboohr","/job/","/jobs/","jobvite","smartrecruiters")):
            _add(out, normalize_link(base_url, href), text, a); continue
        if kws.search(text):
            _add(out, normalize_link(base_url, href), text, a)
    # also JSON-LD
    out += _extract_jsonld_jobs(soup, base_url)
    return out

# --- Collibra ---
def extract_collibra_jobs(soup, page, base_url):
    # Collibra frequently embeds Workday/Greenhouse or has job cards.
    return _generic_anchor_kw(soup, base_url, r"\b(data|engineer|catalog|governance)\b")

# --- Cloudera ---
def extract_cloudera_jobs(soup, page, base_url):
    out = _generic_anchor_kw(soup, base_url, r"\b(data|engineer|cloudera)\b")
    # attempt to find Workday jobIds in scripts
    for s in soup.find_all("script"):
        txt = s.string or s.text or ""
        for m in re.finditer(r'jobId["\']?\s*:\s*["\']?([A-Za-z0-9\-]+)["\']?', txt):
            jid = m.group(1)
            link = f"https://cloudera.wd5.myworkdayjobs.com/External_Career/job/{jid}"
            _add(out, link, f"Cloudera job {jid}", s)
    return out

# --- Pentaho / Hitachi Vantara ---
def extract_pentaho_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(job|careers|position|hitachivantara)\b")

# --- Couchbase ---
def extract_couchbase_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(couchbase|engineer|data)\b")

# --- Exasol ---
def extract_exasol_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(data|engineer|career|job)\b")

# --- Data.World ---
def extract_dataworld_jobs(soup, page, base_url):
    out = []
    # Look for custom dw-job-card or data-job attributes
    for tag in soup.find_all(True):
        if tag.name == "dw-job-card" or tag.get("data-job"):
            a = tag.find("a", href=True)
            if a:
                _add(out, normalize_link(base_url, a.get("href")), a.get_text(" ", strip=True), tag)
    out += _generic_anchor_kw(soup, base_url, r"\b(careers|jobs|data|engineer)\b")
    return out

# --- Sifflet (welcometothejungle) ---
def extract_sifflet_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(data|engineer|sifflet)\b")

# --- Syniti ---
def extract_syniti_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(syniti|workday|engineer|data)\b")

# --- Teradata ---
def extract_teradata_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(teradata|engineer|data|careers)\b")

# --- Qlik ---
def extract_qlik_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(qlik|careerhub|engineer|data)\b")

# --- Atlan ---
def extract_atlan_jobs(soup, page, base_url):
    # Atlan often includes ashby/greenhouse/lever embeds; prioritize anchors with data keywords
    return _generic_anchor_kw(soup, base_url, r"\b(careers|data|engineer|connector)\b")

# --- BigEye ---
def extract_bigeye_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(data|engineer|bigeye|observab)\b")

# --- Alation ---
def extract_alation_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(data|workday|engineer|catalog)\b")

# --- Decube ---
def extract_decube_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(job|career|developer|engineer)\b")

# --- Firebolt ---
def extract_firebolt_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(firebolt|engineer|data|sql|warehouse)\b")

# --- Solidatus ---
def extract_solidatus_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(solidatus|bamboohr|data|engineer)\b")

# --- Vertica ---
def extract_vertica_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(vertica|engineer|data|jobs)\b")

# --- Yellowbrick ---
def extract_yellowbrick_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(yellowbrick|engineer|data|careers)\b")

# --- InfluxData ---
def extract_influxdata_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(influx|influxdata|engineer|data|time series)\b")

# --- Ataccama ---
def extract_ataccama_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(ataccama|data|governance|engineer)\b")

# --- Datadog ---
def extract_datadog_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(datadog|observab|monitor|engineer|sre)\b")

# --- Amazon ---
def extract_amazon_jobs(soup, page, base_url):
    # amazon.jobs is noisy; prefer anchors mentioning aws, redshift, kinesis, data
    return _generic_anchor_kw(soup, base_url, r"\b(aws|redshift|kinesis|data engineer|data|etl)\b")

# --- IBM ---
def extract_ibm_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(ibm|watson|cloud pak|data|engineer)\b")

# --- Oracle ---
def extract_oracle_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(oracle|autonomous|oci|cloud|engineer)\b")

# --- Informatica ---
def extract_informatica_jobs(soup, page, base_url):
    return _generic_anchor_kw(soup, base_url, r"\b(informatica|gr8people|data|etl|connector)\b")

# ----------------- Registry -----------------
SPECIAL_EXTRACTORS_DEEP = {
    "Collibra": extract_collibra_jobs,
    "Cloudera": extract_cloudera_jobs,
    "Pentaho": extract_pentaho_jobs,
    "Couchbase": extract_couchbase_jobs,
    "Exasol": extract_exasol_jobs,
    "Data.World": extract_dataworld_jobs,
    "Sifflet": extract_sifflet_jobs,
    "Syniti": extract_syniti_jobs,
    "Teradata": extract_teradata_jobs,
    "Qlik": extract_qlik_jobs,
    "Atlan": extract_atlan_jobs,
    "BigEye": extract_bigeye_jobs,
    "Alation": extract_alation_jobs,
    "Decube": extract_decube_jobs,
    "Firebolt": extract_firebolt_jobs,
    "Solidatus": extract_solidatus_jobs,
    "Vertica": extract_vertica_jobs,
    "Yellowbrick": extract_yellowbrick_jobs,
    "InfluxData": extract_influxdata_jobs,
    "Ataccama": extract_ataccama_jobs,
    "Datadog": extract_datadog_jobs,
    "Amazon": extract_amazon_jobs,
    "IBM": extract_ibm_jobs,
    "Oracle": extract_oracle_jobs,
    "Informatica": extract_informatica_jobs,
}

# End of module
