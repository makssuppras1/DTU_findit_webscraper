"""
Compare how different faculties (publishers) write theses: text stats and distinctive vocabulary.

Uses abstract_ts by default; use --fulltext-gcs to analyze full-text markdown from GCS instead.

Run: python analysis/faculty_writing_analysis.py [--fulltext-gcs] [--data path]
"""
from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_PATH = "Thesis_meta/thesis_meta_combined_filtered.csv"
RESULTS_DIR = Path("analysis/results")
FIGURES_DIR = Path("analysis/figures")
MIN_TEXT_LEN = 50
FULLTEXT_MARKDOWN_PREFIX = "dtu_findit/master_thesis_markdown"

ABSTRACT_COL_CANDIDATES = ["abstract_ts", "abstract", "Abstract", "description"]
PUBLISHER_COL_CANDIDATES = ["Publisher", "publisher", "section", "Section", "department", "Department"]
ID_COL_CANDIDATES = ["ID", "id", "member_id_ss", "thesis_id", "record_id"]


def _list_markdown_blobs(bucket, prefix: str) -> dict[str, str]:
    prefix = prefix.rstrip("/") + "/"
    out = {}
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith(".md"):
            continue
        stem = Path(blob.name).stem
        if stem not in out:
            out[stem] = blob.name
    return out


def _blob_for_row_id(stem_to_blob: dict[str, str], row_id: str) -> str | None:
    row_id = row_id.strip()
    if row_id in stem_to_blob:
        return stem_to_blob[row_id]
    for stem, blob_name in stem_to_blob.items():
        if stem.startswith(row_id + "_"):
            return blob_name
    return None


def _load_fulltext_from_gcs(df: pd.DataFrame, id_col: str, bucket, prefix: str) -> tuple[pd.DataFrame, list[str]]:
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = lambda x, **kw: x
    stem_to_blob = _list_markdown_blobs(bucket, prefix)
    log.info("Found %d markdown blobs under gs://%s/%s", len(stem_to_blob), bucket.name, prefix)
    indices = []
    texts = []
    for i, row in tqdm(df.iterrows(), total=len(df), desc="Loading fulltext from GCS"):
        raw = row.get(id_col)
        if pd.isna(raw) or not str(raw).strip():
            continue
        for rid in str(raw).strip().split("|"):
            blob_name = _blob_for_row_id(stem_to_blob, rid.strip())
            if blob_name is None:
                continue
            try:
                content = bucket.blob(blob_name).download_as_bytes().decode("utf-8", errors="replace")
            except Exception as e:
                log.warning("Failed to download %s: %s", blob_name, e)
                continue
            if len(content.strip()) < MIN_TEXT_LEN:
                continue
            indices.append(i)
            texts.append(content.strip())
            break
    filtered = df.loc[indices].reset_index(drop=True)
    log.info("Loaded full text for %d theses", len(texts))
    return filtered, texts


def detect_schema(df: pd.DataFrame) -> dict[str, str]:
    schema = {}
    for col in df.columns:
        c = str(col).strip()
        if not schema.get("abstract_col") and any(c == x or (x in c and "abstract" in c.lower()) for x in ABSTRACT_COL_CANDIDATES):
            schema["abstract_col"] = col
        if not schema.get("publisher_col") and any(c == x or x.lower() in c.lower() for x in PUBLISHER_COL_CANDIDATES):
            schema["publisher_col"] = col
        if not schema.get("id_col") and any(c == x or x.lower() in c.lower() for x in ID_COL_CANDIDATES):
            schema["id_col"] = col
    if not schema.get("abstract_col"):
        schema["abstract_col"] = df.columns[0]
    if not schema.get("publisher_col"):
        schema["publisher_col"] = None
    if not schema.get("id_col"):
        schema["id_col"] = df.columns[0]
    return schema


def _words(s: str) -> list[str]:
    return re.findall(r"\b\w+\b", s.lower())


def _sentences(s: str) -> list[str]:
    return [t.strip() for t in re.split(r"[.!?]+", s) if t.strip()]


def text_stats(text: str) -> dict:
    words = _words(text)
    sents = _sentences(text)
    nw = len(words)
    ns = len(sents) or 1
    return {
        "word_count": nw,
        "sentence_count": ns,
        "words_per_sentence": nw / ns if ns else 0,
        "type_token_ratio": len(set(words)) / nw if nw else 0,
    }


def load_data(
    path: Path,
    min_len: int = MIN_TEXT_LEN,
    use_fulltext_gcs: bool = False,
    fulltext_bucket: str | None = None,
    fulltext_prefix: str | None = None,
    sample_size: int | None = None,
) -> tuple[pd.DataFrame, list[str], str]:
    """Load meta and texts (abstract or full text). Returns (df, texts, text_source_label)."""
    RANDOM_STATE = 42
    df = pd.read_csv(path, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
    schema = detect_schema(df)
    abstract_col = schema["abstract_col"]
    publisher_col = schema.get("publisher_col")
    id_col = schema.get("id_col")
    if not publisher_col or publisher_col not in df.columns:
        raise ValueError("Publisher column required for faculty analysis")
    df[abstract_col] = df[abstract_col].astype(str).str.strip().replace("nan", "")
    df = df[df[abstract_col].str.len() >= min_len]
    df = df.dropna(subset=[publisher_col])
    df = df[df[publisher_col].astype(str).str.strip() != ""]

    if use_fulltext_gcs:
        bucket_name = fulltext_bucket or os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
        prefix = fulltext_prefix or FULLTEXT_MARKDOWN_PREFIX
        try:
            from google.cloud import storage
            bucket = storage.Client().bucket(bucket_name)
        except ImportError:
            raise ImportError("Full-text from GCS requires google-cloud-storage") from None
        # Match markdown blobs by member_id_ss (same as PDF bucket); fall back to id_col
        fulltext_id_col = "member_id_ss" if "member_id_ss" in df.columns else id_col
        df, texts = _load_fulltext_from_gcs(df, fulltext_id_col, bucket, prefix)
        if len(df) == 0:
            raise SystemExit("No rows with markdown full text found.")
        source_label = "full_text"
    else:
        texts = df[abstract_col].tolist()
        source_label = "abstract"

    if sample_size and len(df) > sample_size:
        df = df.reset_index(drop=True)
        idx = df.sample(n=sample_size, random_state=RANDOM_STATE).index.tolist()
        df = df.loc[idx].reset_index(drop=True)
        texts = [texts[i] for i in idx]
        log.info("Sampled to %d theses", len(df))
    return df, texts, source_label


def faculty_stats(df: pd.DataFrame, texts: list[str], publisher_col: str) -> pd.DataFrame:
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = lambda x, **kw: x
    rows = []
    for pub in tqdm(df[publisher_col].dropna().unique(), desc="Faculty stats"):
        pub = str(pub).strip()
        if not pub:
            continue
        mask = df[publisher_col].astype(str).str.strip() == pub
        sub_texts = [texts[i] for i in range(len(texts)) if mask.iloc[i]]
        if not sub_texts:
            continue
        stats_list = [text_stats(t) for t in sub_texts]
        n = len(stats_list)
        rows.append({
            "faculty": pub,
            "n_theses": n,
            "mean_word_count": sum(s["word_count"] for s in stats_list) / n,
            "median_word_count": pd.Series([s["word_count"] for s in stats_list]).median(),
            "mean_sentence_count": sum(s["sentence_count"] for s in stats_list) / n,
            "mean_words_per_sentence": sum(s["words_per_sentence"] for s in stats_list) / n,
            "mean_type_token_ratio": sum(s["type_token_ratio"] for s in stats_list) / n,
        })
    return pd.DataFrame(rows).sort_values("n_theses", ascending=False)


def faculty_distinctive_terms(df: pd.DataFrame, texts: list[str], publisher_col: str, top_n: int = 20) -> pd.DataFrame:
    """One document per faculty (concatenated texts); TF-IDF; top terms per faculty."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = lambda x, **kw: x
    faculty_docs = []
    faculty_names = []
    for pub in tqdm(df[publisher_col].dropna().unique(), desc="Building faculty docs"):
        pub = str(pub).strip()
        if not pub:
            continue
        mask = df[publisher_col].astype(str).str.strip() == pub
        sub_texts = [texts[i] for i in range(len(texts)) if mask.iloc[i]]
        if not sub_texts:
            continue
        faculty_docs.append(" ".join(sub_texts))
        faculty_names.append(pub)

    if len(faculty_docs) < 2:
        log.warning("Need at least 2 faculties for distinctive terms")
        return pd.DataFrame()

    vectorizer = TfidfVectorizer(max_features=5000, stop_words="english", min_df=1)
    X = vectorizer.fit_transform(faculty_docs)
    feature_names = vectorizer.get_feature_names_out()
    rows = []
    for i, name in tqdm(enumerate(faculty_names), total=len(faculty_names), desc="Distinctive terms"):
        row = X.getrow(i).toarray().flatten()
        idx = np.argsort(-row)[:top_n]
        top = [feature_names[j] for j in idx if row[j] > 0]
        rows.append({"faculty": name, "top_terms": "|".join(top)})
    return pd.DataFrame(rows)


def run(
    data_path: Path,
    use_fulltext_gcs: bool = False,
    fulltext_bucket: str | None = None,
    fulltext_prefix: str | None = None,
    sample_size: int | None = None,
) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading data from %s (fulltext=%s)", data_path, use_fulltext_gcs)
    df, texts, source_label = load_data(
        data_path,
        use_fulltext_gcs=use_fulltext_gcs,
        fulltext_bucket=fulltext_bucket,
        fulltext_prefix=fulltext_prefix,
        sample_size=sample_size,
    )
    schema = detect_schema(df)
    publisher_col = schema["publisher_col"]
    log.info("Loaded %d theses (%s), %d faculties", len(df), source_label, df[publisher_col].nunique())

    stats_df = faculty_stats(df, texts, publisher_col)
    out_stats = RESULTS_DIR / "faculty_writing_stats.csv"
    stats_df.to_csv(out_stats, index=False)
    log.info("Saved %s", out_stats)

    distinct_df = faculty_distinctive_terms(df, texts, publisher_col, top_n=20)
    if not distinct_df.empty:
        out_terms = RESULTS_DIR / "faculty_distinctive_terms.csv"
        distinct_df.to_csv(out_terms, index=False)
        log.info("Saved %s", out_terms)

    # Figures: mean words per sentence and type-token ratio by faculty
    import matplotlib.pyplot as plt
    stats_sorted = stats_df.sort_values("mean_words_per_sentence", ascending=True)
    fig, ax = plt.subplots(figsize=(10, max(5, len(stats_sorted) * 0.22)))
    ax.barh(stats_sorted["faculty"], stats_sorted["mean_words_per_sentence"], color="steelblue", alpha=0.8)
    ax.set_xlabel("Mean words per sentence")
    ax.set_title(f"Faculty writing: sentence length ({source_label})")
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "faculty_mean_words_per_sentence.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved %s", FIGURES_DIR / "faculty_mean_words_per_sentence.png")

    stats_sorted_ttr = stats_df.sort_values("mean_type_token_ratio", ascending=True)
    fig2, ax2 = plt.subplots(figsize=(10, max(5, len(stats_sorted_ttr) * 0.22)))
    ax2.barh(stats_sorted_ttr["faculty"], stats_sorted_ttr["mean_type_token_ratio"], color="seagreen", alpha=0.8)
    ax2.set_xlabel("Mean type-token ratio (vocabulary diversity)")
    ax2.set_title(f"Faculty writing: vocabulary diversity ({source_label})")
    fig2.tight_layout()
    fig2.savefig(FIGURES_DIR / "faculty_type_token_ratio.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved %s", FIGURES_DIR / "faculty_type_token_ratio.png")


def main():
    parser = argparse.ArgumentParser(description="Compare thesis writing across faculties")
    parser.add_argument("--data", type=Path, default=Path(DATA_PATH), help="Path to thesis meta CSV")
    parser.add_argument("--fulltext-gcs", action="store_true", help="Use full-text markdown from GCS")
    parser.add_argument("--fulltext-bucket", default=None)
    parser.add_argument("--fulltext-prefix", default=None)
    parser.add_argument("--sample-size", type=int, default=None, help="Use at most N theses (for testing fulltext)")
    args = parser.parse_args()
    run(
        data_path=args.data,
        use_fulltext_gcs=args.fulltext_gcs,
        fulltext_bucket=args.fulltext_bucket,
        fulltext_prefix=args.fulltext_prefix,
        sample_size=args.sample_size,
    )


if __name__ == "__main__":
    main()
