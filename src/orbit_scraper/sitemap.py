import re
import xml.etree.ElementTree as ET
import requests

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; DTU-Orbit-Scraper/1.0)"
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
SITEMAP_URL = "https://orbit.dtu.dk/sitemap/persons.xml"


def fetch_sitemap_page(session: requests.Session, n: int) -> str | None:
    resp = session.get(f"{SITEMAP_URL}?n={n}", timeout=30)
    if not resp.ok or not resp.text.strip():
        return None
    return resp.text


def parse_urls_from_sitemap(xml_text: str) -> list[str]:
    urls = []
    try:
        root = ET.fromstring(xml_text)
        for loc in root.findall(f".//{{{NS}}}loc"):
            if loc is not None and loc.text:
                url = loc.text.strip()
                if url and "/persons/" in url:
                    urls.append(url)
    except ET.ParseError:
        pass
    if not urls:
        for match in re.findall(r"<loc>([^<]+/persons/[^<]+)</loc>", xml_text):
            urls.append(match.strip())
    return urls


def discover_person_urls(
    session: requests.Session | None = None,
    max_sitemaps: int = 500,
) -> list[str]:
    session = session or requests.Session()
    session.headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
    seen: set[str] = set()
    n = 1
    while n <= max_sitemaps:
        xml_text = fetch_sitemap_page(session, n)
        if not xml_text:
            break
        urls = parse_urls_from_sitemap(xml_text)
        if not urls:
            break
        for u in urls:
            u = u.rstrip("/") or u
            seen.add(u if u.endswith("/") else u + "/")
        n += 1
    result = sorted(seen)
    return result
