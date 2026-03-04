#!/usr/bin/env bash
# Download findit.dtu.dk CSV exports by page into a folder.
# Usage: ./scripts/download_findit_csv.sh [OUTPUT_DIR]
# Default output dir: findit_downloads

OUT_DIR="${1:-findit_downloads}"
mkdir -p "$OUT_DIR"

for i in $(seq 1 215588); do
  curl -s -o "${OUT_DIR}/page_${i}.csv" --cookie "shunt_hint=anonymous" \
    'https://findit.dtu.dk/en/catalog/download.csv?access_type=dtu&button=&configuration=custom&fields%5B%5D=abstract_ts&fields%5B%5D=access_ss&fields%5B%5D=affiliation_ts&fields%5B%5D=alert_timestamp_dt&fields%5B%5D=author_ts&fields%5B%5D=citation_count_i&fields%5B%5D=cluster_id_ss&fields%5B%5D=dtu_library_collection_facet&fields%5B%5D=collection_facet&fields%5B%5D=pub_date_tis&fields%5B%5D=conf_title_ts&fields%5B%5D=doi_ss&fields%5B%5D=editor_ts&fields%5B%5D=embargo_ssf&fields%5B%5D=format&fields%5B%5D=fulltext_availability_facet&fields%5B%5D=has_openaccess_fulltext_b&fields%5B%5D=holdings_ssf&fields%5B%5D=isbn_ss&fields%5B%5D=journal_issue_ssf&fields%5B%5D=journal_issue_tsort&fields%5B%5D=journal_oa_model_ss&fields%5B%5D=journal_page_ssf&fields%5B%5D=journal_page_start_tsort&fields%5B%5D=journal_title_ts&fields%5B%5D=journal_title_facet&fields%5B%5D=toc_key_s&fields%5B%5D=journal_vol_ssf&fields%5B%5D=journal_vol_tsort&fields%5B%5D=keywords_ts&fields%5B%5D=keywords_facet&fields%5B%5D=keywords_normalized&fields%5B%5D=isolanguage_facet&fields%5B%5D=member_id_ss&fields%5B%5D=orcid_ss&fields%5B%5D=primary_member_id_s&fields%5B%5D=publisher_ts&fields%5B%5D=source_ss&fields%5B%5D=source_all_ss&fields%5B%5D=title_ts&fulltext_token=7dc3fe63db1ea062930311ad84b42846&page='"$i"'&per_page=1000&separator=%3B&sort=id+asc&utf8=%E2%9C%93'
done
