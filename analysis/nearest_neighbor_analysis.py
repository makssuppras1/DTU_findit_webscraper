"""
Nearest-neighbor analysis: top-k similar theses per thesis, similarity distributions by department.

Requires: thesis_semantic_clustering.py run first (clustering_labels.csv + embeddings cache).

Run: python analysis/nearest_neighbor_analysis.py [--fulltext] [--top-k 10]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESULTS_DIR = Path("analysis/results")
CACHE_DIR = Path("analysis/cache")
CLUSTERING_LABELS_PATH = RESULTS_DIR / "clustering_labels.csv"
EMBEDDINGS_PATH_ABSTRACT = CACHE_DIR / "abstract_embeddings.npy"
EMBEDDINGS_PATH_FULLTEXT = CACHE_DIR / "fulltext_embeddings.npy"


def load_labels_and_embeddings(use_fulltext: bool = False) -> tuple[pd.DataFrame, np.ndarray]:
    df = pd.read_csv(CLUSTERING_LABELS_PATH, sep=",", encoding="utf-8", low_memory=False)
    emb_path = EMBEDDINGS_PATH_FULLTEXT if use_fulltext else EMBEDDINGS_PATH_ABSTRACT
    if not emb_path.exists():
        raise FileNotFoundError(f"Missing embeddings: {emb_path}")
    emb = np.load(emb_path)
    if len(df) != len(emb):
        raise ValueError(f"Row count mismatch: labels {len(df)} vs embeddings {len(emb)}")
    return df, emb


def run(top_k: int = 10, use_fulltext: bool = False) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    labels_df, embeddings = load_labels_and_embeddings(use_fulltext=use_fulltext)
    id_col = labels_df.columns[0]
    publisher_col = labels_df.columns[1] if labels_df.shape[1] > 1 else None

    # Top-k NN (excluding self): use k+1 and drop first
    n_neighbors = min(top_k + 1, len(embeddings))
    nn = NearestNeighbors(n_neighbors=n_neighbors, metric="cosine", algorithm="brute")
    nn.fit(embeddings)
    dists, idx = nn.kneighbors(embeddings)
    # cosine_sim = 1 - cosine_dist for normalized vectors
    sims = 1 - dists

    ids = labels_df[id_col].astype(str).values
    rows = []
    for i in tqdm(range(len(ids)), desc="Nearest neighbors"):
        for r in range(1, idx.shape[1]):
            j = idx[i, r]
            rows.append({
                "thesis_id": ids[i],
                "rank": r,
                "neighbor_id": ids[j],
                "cosine_similarity": round(float(sims[i, r]), 4),
            })
    nn_df = pd.DataFrame(rows)
    out_nn = RESULTS_DIR / "nearest_neighbors.csv"
    nn_df.to_csv(out_nn, index=False)
    log.info("Saved %s", out_nn)

    # Per-thesis mean similarity to top-k neighbors (for department stats)
    mean_sim = sims[:, 1:].mean(axis=1)
    labels_df = labels_df.copy()
    labels_df["mean_nn_similarity"] = mean_sim

    if publisher_col:
        dept_stats = labels_df.groupby(publisher_col).agg(
            n_theses=("mean_nn_similarity", "count"),
            mean_nn_similarity=("mean_nn_similarity", "mean"),
            median_nn_similarity=("mean_nn_similarity", "median"),
        ).reset_index()
        dept_stats.to_csv(RESULTS_DIR / "department_nn_similarity_stats.csv", index=False)
        log.info("Saved department_nn_similarity_stats.csv")


def main():
    parser = argparse.ArgumentParser(description="Nearest-neighbor analysis")
    parser.add_argument("--fulltext", action="store_true", help="Use fulltext embeddings cache")
    parser.add_argument("--top-k", type=int, default=10, help="Number of nearest neighbors per thesis")
    args = parser.parse_args()
    run(top_k=args.top_k, use_fulltext=args.fulltext)


if __name__ == "__main__":
    main()
