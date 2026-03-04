import argparse
import pandas as pd

from .scraper import scrape_persons

COLUMNS = [
    "url", "name", "affiliations", "email", "orcid", "website",
    "address", "profile_text", "keywords", "sdgs",
]


def main() -> None:
    p = argparse.ArgumentParser(description="Scrape DTU Orbit person profiles to CSV")
    p.add_argument("--out", default="dtu_orbit_persons.csv", help="Output CSV path")
    p.add_argument("--limit", type=int, default=None, help="Max number of profiles to scrape")
    p.add_argument("--sleep", type=float, default=0.2, help="Seconds between requests")
    p.add_argument("--max-sitemaps", type=int, default=2000, help="Max sitemap pages to fetch")
    p.add_argument("--retries", type=int, default=2, help="Fetch attempts per profile")
    p.add_argument("--failed-urls", default="orbit_failed_urls.txt", help="File to write failed URLs (empty to disable)")
    args = p.parse_args()

    records = scrape_persons(
        out_path=args.out,
        limit=args.limit,
        sleep=args.sleep,
        max_sitemaps=args.max_sitemaps,
        retries=args.retries,
        failed_urls_path=args.failed_urls if args.failed_urls else None,
    )
    df = pd.DataFrame([r.to_row() for r in records], columns=COLUMNS)
    df.to_csv(args.out, index=False, encoding="utf-8")


if __name__ == "__main__":
    main()
