"""
Faculty-level analyses from semantic clustering: topic concentration, noise share,
faculty similarity, affinity to topics, exemplar theses, temporal trends, keyword usage.

Requires: run thesis_semantic_clustering.py first to produce clustering_labels.csv and embeddings cache.

Run: python analysis/faculty_clustering_analysis.py [--fulltext]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESULTS_DIR = Path("analysis/results")
FIGURES_DIR = Path("analysis/figures")
CACHE_DIR = Path("analysis/cache")
CLUSTERING_LABELS_PATH = RESULTS_DIR / "clustering_labels.csv"
EMBEDDINGS_PATH_ABSTRACT = CACHE_DIR / "abstract_embeddings.npy"
EMBEDDINGS_PATH_FULLTEXT = CACHE_DIR / "fulltext_embeddings.npy"
DATA_PATH = "Thesis_meta/thesis_meta_combined_filtered.csv"
KEYWORDS_FOR_USAGE = [
    "wind", "turbine", "machine", "learning", "optimization", "renewable",
    "hydrogen", "sustainability", "building", "energy", "model", "data",
    "sensor", "algorithm", "simulation", "neural", "deep",
]


def _detect_cols(df: pd.DataFrame) -> dict:
    # clustering_labels.csv: id_col, publisher_col, cluster_id [, year]
    out = {"id": df.columns[0], "publisher": df.columns[1], "cluster_id": "cluster_id"}
    if "cluster_id" not in df.columns:
        for c in df.columns:
            if "cluster" in str(c).lower():
                out["cluster_id"] = c
                break
    if "year" in df.columns:
        out["year"] = "year"
    return out


def load_labels_and_embeddings(use_fulltext: bool = False) -> tuple[pd.DataFrame, np.ndarray]:
    labels_path = Path(CLUSTERING_LABELS_PATH)
    if not labels_path.exists():
        raise FileNotFoundError(f"Run thesis_semantic_clustering.py first. Missing {labels_path}")
    df = pd.read_csv(labels_path, sep=",", encoding="utf-8", low_memory=False)
    emb_path = EMBEDDINGS_PATH_FULLTEXT if use_fulltext else EMBEDDINGS_PATH_ABSTRACT
    if not emb_path.exists():
        raise FileNotFoundError(f"Missing embeddings: {emb_path}")
    emb = np.load(emb_path)
    if len(df) != len(emb):
        raise ValueError(f"Row count mismatch: labels {len(df)} vs embeddings {len(emb)}")
    return df, emb


def topic_concentration_and_noise(labels_df: pd.DataFrame, publisher_col: str, cluster_col: str) -> pd.DataFrame:
    rows = []
    for pub in tqdm(labels_df[publisher_col].dropna().unique(), desc="Topic concentration"):
        sub = labels_df[labels_df[publisher_col] == pub]
        n = len(sub)
        noise_count = (sub[cluster_col] == -1).sum()
        noise_share = noise_count / n if n else 0
        dist = sub[cluster_col].value_counts()
        dist = dist[dist.index != -1]
        if len(dist) == 0:
            entropy = 0.0
            herfindahl = 0.0
        else:
            p = dist / dist.sum()
            from scipy.stats import entropy as scipy_entropy
            entropy = float(scipy_entropy(p))
            herfindahl = float((p ** 2).sum())
        rows.append({
            "faculty": pub,
            "n_theses": n,
            "noise_count": int(noise_count),
            "noise_share": round(noise_share, 4),
            "topic_entropy": round(entropy, 4),
            "topic_concentration_herfindahl": round(herfindahl, 4),
        })
    return pd.DataFrame(rows).sort_values("n_theses", ascending=False)


def faculty_similarity(labels_df: pd.DataFrame, publisher_col: str, cluster_col: str) -> pd.DataFrame:
    counts = labels_df.groupby([publisher_col, cluster_col]).size().unstack(fill_value=0)
    totals = counts.sum(axis=1)
    norm = counts.div(totals, axis=0)
    corr = norm.T.corr()
    return corr


def faculty_affinity_to_topics(embeddings: np.ndarray, labels_df: pd.DataFrame, publisher_col: str, cluster_col: str) -> pd.DataFrame:
    unique_clusters = sorted(c for c in labels_df[cluster_col].unique() if c != -1)
    cluster_centroids = {}
    for c in unique_clusters:
        mask = labels_df[cluster_col].values == c
        cluster_centroids[c] = embeddings[mask].mean(axis=0)
    faculty_centroids = {}
    for pub in tqdm(labels_df[publisher_col].dropna().unique(), desc="Faculty centroids"):
        mask = labels_df[publisher_col].values == pub
        faculty_centroids[pub] = embeddings[mask].mean(axis=0)
    rows = []
    for pub in tqdm(faculty_centroids, desc="Affinity to topics"):
        fc = faculty_centroids[pub]
        d = {}
        for c in unique_clusters:
            d[f"cluster_{c}"] = float(np.linalg.norm(fc - cluster_centroids[c]))
        rows.append({"faculty": pub, **d})
    return pd.DataFrame(rows)


def exemplar_theses(
    embeddings: np.ndarray,
    labels_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    id_col: str,
    publisher_col: str,
    cluster_col: str,
    abstract_col: str | None,
    top_k: int = 3,
) -> pd.DataFrame:
    id_in_labels = labels_df.columns[0] if id_col not in labels_df.columns else id_col
    unique_clusters = sorted(c for c in labels_df[cluster_col].unique() if c != -1)
    rows = []
    for pub in tqdm(labels_df[publisher_col].dropna().unique(), desc="Exemplar theses"):
        pub_mask = labels_df[publisher_col] == pub
        for c in unique_clusters:
            cl_mask = labels_df[cluster_col] == c
            mask = pub_mask & cl_mask
            if mask.sum() == 0:
                continue
            idx = np.where(mask)[0]
            cent = embeddings[mask].mean(axis=0)
            dists = np.linalg.norm(embeddings[mask] - cent, axis=1)
            order = np.argsort(dists)[:top_k]
            for i, pos in enumerate(idx[order]):
                thesis_id = labels_df.iloc[pos][id_in_labels]
                abstr = ""
                if abstract_col and abstract_col in meta_df.columns:
                    m = meta_df[meta_df[id_col].astype(str) == str(thesis_id)]
                    if len(m):
                        abstr = (m[abstract_col].iloc[0] or "")[:200]
                rows.append({
                    "faculty": pub,
                    "cluster_id": int(c),
                    "rank": i + 1,
                    "thesis_id": thesis_id,
                    "abstract_snippet": abstr[:200] + "..." if len(abstr) > 200 else abstr,
                })
    return pd.DataFrame(rows)


def temporal_topic_distribution(labels_df: pd.DataFrame, publisher_col: str, cluster_col: str, year_col: str | None) -> pd.DataFrame | None:
    if year_col not in labels_df.columns or labels_df[year_col].isna().all():
        return None
    labels_df = labels_df[labels_df[cluster_col] != -1].copy()
    if len(labels_df) == 0:
        return None
    labels_df["year"] = pd.to_numeric(labels_df[year_col], errors="coerce")
    labels_df = labels_df.dropna(subset=["year"])
    if len(labels_df) == 0:
        return None
    counts = labels_df.groupby([publisher_col, "year", cluster_col]).size().unstack(fill_value=0)
    totals = counts.sum(axis=1)
    norm = counts.div(totals, axis=0)
    return norm.reset_index()


def keyword_usage_by_faculty(meta_df: pd.DataFrame, labels_df: pd.DataFrame, id_col: str, publisher_col: str, abstract_col: str) -> pd.DataFrame:
    id_in_labels = labels_df.columns[0]
    merged = labels_df[[id_in_labels, publisher_col]].merge(
        meta_df[[id_col, abstract_col]].astype({abstract_col: str}),
        left_on=id_in_labels,
        right_on=id_col,
        how="inner",
    )
    from sklearn.feature_extraction.text import TfidfVectorizer
    rows = []
    for pub in tqdm(merged[publisher_col].dropna().unique(), desc="Keyword usage"):
        docs = merged.loc[merged[publisher_col] == pub, abstract_col].fillna("").tolist()
        if not docs:
            continue
        vec = TfidfVectorizer(vocabulary=KEYWORDS_FOR_USAGE, lowercase=True)
        try:
            X = vec.fit_transform(docs)
        except Exception:
            continue
        sums = np.asarray(X.sum(axis=0)).flatten()
        for i, term in enumerate(KEYWORDS_FOR_USAGE):
            if term in vec.vocabulary_:
                j = vec.vocabulary_[term]
                rows.append({"faculty": pub, "keyword": term, "tfidf_sum": float(sums[j]), "n_docs": len(docs)})
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def run(use_fulltext: bool = False, data_path: Path | None = None) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    data_path = data_path or Path(DATA_PATH)

    labels_df, embeddings = load_labels_and_embeddings(use_fulltext=use_fulltext)
    cols = _detect_cols(labels_df)
    publisher_col = cols["publisher"]
    cluster_col = cols["cluster_id"]
    id_col = cols["id"]
    year_col = cols.get("year") if "year" in cols and cols["year"] in labels_df.columns else None
    if year_col is None and "year" in labels_df.columns:
        year_col = "year"

    # 1. Topic concentration + noise share
    conc_df = topic_concentration_and_noise(labels_df, publisher_col, cluster_col)
    conc_df.to_csv(RESULTS_DIR / "faculty_topic_concentration_noise.csv", index=False)
    log.info("Saved faculty_topic_concentration_noise.csv")

    # 2. Faculty similarity (topic profiles)
    sim_df = faculty_similarity(labels_df, publisher_col, cluster_col)
    sim_df.to_csv(RESULTS_DIR / "faculty_topic_similarity.csv")
    import matplotlib.pyplot as plt
    import seaborn as sns
    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(sim_df, ax=ax, cmap="RdBu_r", center=0, vmin=-0.5, vmax=1)
    ax.set_title("Faculty similarity (topic profile correlation)")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "faculty_topic_similarity_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved faculty_topic_similarity_heatmap.png")

    # 3. Faculty affinity to cluster centroids
    aff_df = faculty_affinity_to_topics(embeddings, labels_df, publisher_col, cluster_col)
    aff_df.to_csv(RESULTS_DIR / "faculty_affinity_to_clusters.csv", index=False)
    log.info("Saved faculty_affinity_to_clusters.csv")

    # 4. Exemplar theses per faculty per cluster
    meta_df = pd.read_csv(data_path, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
    abstract_col = None
    for c in meta_df.columns:
        if "abstract" in str(c).lower():
            abstract_col = c
            break
    id_meta = meta_df.columns[0] if id_col not in meta_df.columns else id_col
    ex_df = exemplar_theses(embeddings, labels_df, meta_df, id_meta, publisher_col, cluster_col, abstract_col, top_k=2)
    ex_df.to_csv(RESULTS_DIR / "faculty_cluster_exemplars.csv", index=False)
    log.info("Saved faculty_cluster_exemplars.csv")

    # 5. Temporal (if year available)
    if year_col and year_col in labels_df.columns:
        temporal_df = temporal_topic_distribution(labels_df, publisher_col, cluster_col, year_col)
        if temporal_df is not None and len(temporal_df) > 0:
            temporal_df.to_csv(RESULTS_DIR / "faculty_year_topic_distribution.csv", index=False)
            log.info("Saved faculty_year_topic_distribution.csv")
    else:
        log.info("No year column; skipping temporal topic distribution")

    # 6. Keyword usage by faculty
    if abstract_col and data_path.exists():
        kw_df = keyword_usage_by_faculty(meta_df, labels_df, id_meta, publisher_col, abstract_col)
        if len(kw_df) > 0:
            kw_df.to_csv(RESULTS_DIR / "faculty_keyword_usage.csv", index=False)
            log.info("Saved faculty_keyword_usage.csv")

    # Bar chart: topic concentration (entropy) and noise share
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, max(5, len(conc_df) * 0.2)))
    conc_sorted = conc_df.sort_values("topic_entropy", ascending=True)
    ax1.barh(conc_sorted["faculty"], conc_sorted["topic_entropy"], color="steelblue", alpha=0.8)
    ax1.set_xlabel("Topic entropy (higher = more diverse)")
    ax1.set_title("Faculty topic diversity")
    conc_noise = conc_df.sort_values("noise_share", ascending=True)
    ax2.barh(conc_noise["faculty"], conc_noise["noise_share"], color="coral", alpha=0.8)
    ax2.set_xlabel("Noise share (cluster -1)")
    ax2.set_title("Faculty noise share")
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "faculty_topic_diversity_noise.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved faculty_topic_diversity_noise.png")


def main():
    parser = argparse.ArgumentParser(description="Faculty-level clustering analyses")
    parser.add_argument("--fulltext", action="store_true", help="Use fulltext embeddings cache")
    parser.add_argument("--data", type=Path, default=None, help="Path to thesis meta CSV")
    args = parser.parse_args()
    run(use_fulltext=args.fulltext, data_path=args.data)


if __name__ == "__main__":
    main()
