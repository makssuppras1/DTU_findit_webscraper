import time
from tqdm import tqdm
import requests

from .models import PersonRecord
from .profile_parser import parse_profile
from .sitemap import discover_person_urls

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; DTU-Orbit-Scraper/1.0)"


def scrape_persons(
    out_path: str = "dtu_orbit_persons.csv",
    limit: int | None = None,
    sleep: float = 0.2,
    max_sitemaps: int = 2000,
    retries: int = 2,
    failed_urls_path: str | None = "orbit_failed_urls.txt",
) -> list[PersonRecord]:
    session = requests.Session()
    session.headers["User-Agent"] = DEFAULT_USER_AGENT

    urls = discover_person_urls(session=session, max_sitemaps=max_sitemaps)
    if limit is not None:
        urls = urls[:limit]

    records: list[PersonRecord] = []
    failed: list[str] = []
    for url in tqdm(urls, desc="Profiles", unit="person"):
        last_err = None
        for attempt in range(max(1, retries)):
            try:
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                rec = parse_profile(resp.text, url)
                records.append(rec)
                break
            except Exception as e:
                last_err = e
            if sleep > 0 and attempt < retries - 1:
                time.sleep(sleep)
        else:
            failed.append(url)
        if sleep > 0:
            time.sleep(sleep)

    if failed_urls_path and failed:
        with open(failed_urls_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        tqdm.write(f"Wrote {len(failed)} failed URLs to {failed_urls_path}")

    return records
