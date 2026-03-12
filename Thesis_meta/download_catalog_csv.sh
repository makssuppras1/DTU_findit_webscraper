#!/usr/bin/env bash
# Same as the one-liner: download pages 1–34 into Thesis_meta/catalog_csv/, then combine.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/catalog_csv"
mkdir -p "$OUT_DIR"

for i in `seq 1 34`; do
  curl -O -J -o "${OUT_DIR}/page_${i}.csv" --cookie "shunt_hint=anonymous" \
    'https://findit.dtu.dk/en/catalog/download.csv?access_type=dtu&button=&configuration=custom&dtu=student_theses&fields%5B%5D=abstract_ts&fields%5B%5D=access_ss&fields%5B%5D=affiliation_ts&fields%5B%5D=alert_timestamp_dt&fields%5B%5D=author_ts&fields%5B%5D=citation_count_i&fields%5B%5D=cluster_id_ss&fields%5B%5D=dtu_library_collection_facet&fields%5B%5D=collection_facet&fields%5B%5D=pub_date_tis&fields%5B%5D=conf_title_ts&fields%5B%5D=doi&fields%5B%5D=editor_ts&fields%5B%5D=embargo_ssf&fields%5B%5D=format&fields%5B%5D=fulltext_availability_facet&fields%5B%5D=has_openaccess_fulltext_b&fields%5B%5D=holdings_ssf&fields%5B%5D=isbn_ss&fields%5B%5D=journal_issue_ssf&fields%5B%5D=journal_issue_tsort&fields%5B%5D=journal_oa_model_ss&fields%5B%5D=journal_page_ssf&fields%5B%5D=journal_page_start_tsort&fields%5B%5D=journal_title_ts&fields%5B%5D=journal_title_facet&fields%5B%5D=toc_key_s&fields%5B%5D=journal_vol_ssf&fields%5B%5D=journal_vol_tsort&fields%5B%5D=keywords_ts&fields%5B%5D=keywords_facet&fields%5B%5D=keywords_normalized&fields%5B%5D=isolanguage_facet&fields%5B%5D=member_id_ss&fields%5B%5D=orcid_ss&fields%5B%5D=primary_member_id_s&fields%5B%5D=publisher_ts&fields%5B%5D=source_ss&fields%5B%5D=source_all_ss&fields%5B%5D=title_ts&fulltext_token=0760013c9ae62389ff8e4ec5af003147&page='"$i"'&per_page=1000&separator=%3B&sort=id+asc&utf8=%E2%9C%93'
  echo "Saved page $i"
done

# Combine into one CSV (header from first file only)
COMBINED="${OUT_DIR}/catalog_combined.csv"
first="${OUT_DIR}/page_1.csv"
if [[ ! -s "$first" ]]; then
  echo "No page_1.csv; skipping combine"
  exit 1
fi
head -1 "$first" > "$COMBINED"
for i in $(seq 1 34); do
  f="${OUT_DIR}/page_${i}.csv"
  [[ -s "$f" ]] && tail -n +2 "$f" >> "$COMBINED"
done
echo "Done. Combined: ${COMBINED}"
