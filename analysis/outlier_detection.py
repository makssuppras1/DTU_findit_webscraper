"""
Outlier detection: distance-to-centroid, LOF, Isolation Forest. Output outlier scores and top-outlier theses.

Requires: thesis_semantic_clustering.py run first.

Run: python analysis/outlier_detection.py [--fulltext] [--top-pct 5]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
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


def run(use_fulltext: bool = False, top_pct: float = 5.0) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    labels_df, embeddings = load_labels_and_embeddings(use_fulltext=use_fulltext)
    id_col = labels_df.columns[0]
    cluster_col = "cluster_id" if "cluster_id" in labels_df.columns else [c for c in labels_df.columns if "cluster" in c.lower()][0]
    ids = labels_df[id_col].astype(str).values
    labels = labels_df[cluster_col].values

    # Distance to global centroid
    global_centroid = embeddings.mean(axis=0)
    dist_global = np.linalg.norm(embeddings - global_centroid, axis=1)

    # Distance to assigned cluster centroid (noise: use global)
    unique_clusters = sorted(set(labels))
    cluster_centroids = {}
    for c in unique_clusters:
        mask = labels == c
        cluster_centroids[c] = embeddings[mask].mean(axis=0)
    dist_to_centroid = np.zeros(len(embeddings))
    for i in range(len(embeddings)):
        c = labels[i]
        cent = cluster_centroids.get(c, global_centroid)
        dist_to_centroid[i] = np.linalg.norm(embeddings[i] - cent)

    # LOF (contamination="auto" or small value)
    log.info("Computing LOF...")
    lof = LocalOutlierFactor(n_neighbors=20, metric="cosine", novelty=False)
    lof_scores = lof.fit_predict(embeddings)
    lof_neg_scores = -lof.negative_outlier_factor_

    # Isolation Forest
    log.info("Computing Isolation Forest...")
    iso = IsolationForest(random_state=42, contamination=0.05)
    iso_scores = iso.fit_predict(embeddings)
    iso_anomaly = -iso.decision_function(embeddings)

    out_df = pd.DataFrame({
        "thesis_id": ids,
        "cluster_id": labels,
        "dist_to_centroid": np.round(dist_to_centroid, 4),
        "dist_to_global_centroid": np.round(dist_global, 4),
        "lof_score": np.round(lof_neg_scores, 4),
        "isolation_forest_anomaly": np.round(iso_anomaly, 4),
    })
    out_df.to_csv(RESULTS_DIR / "outlier_scores.csv", index=False)
    log.info("Saved outlier_scores.csv")

    # Top outliers by LOF and by isolation_forest
    n_top = max(1, int(len(embeddings) * top_pct / 100))
    out_df_sorted_lof = out_df.nlargest(n_top, "lof_score")[["thesis_id", "cluster_id", "lof_score", "dist_to_centroid"]]
    out_df_sorted_lof.to_csv(RESULTS_DIR / "outliers_top_lof.csv", index=False)
    out_df_sorted_iso = out_df.nlargest(n_top, "isolation_forest_anomaly")[["thesis_id", "cluster_id", "isolation_forest_anomaly", "dist_to_centroid"]]
    out_df_sorted_iso.to_csv(RESULTS_DIR / "outliers_top_isolation_forest.csv", index=False)
    log.info("Saved outliers_top_lof.csv and outliers_top_isolation_forest.csv (top %d%%)", top_pct)


def main():
    parser = argparse.ArgumentParser(description="Outlier detection on thesis embeddings")
    parser.add_argument("--fulltext", action="store_true", help="Use fulltext embeddings cache")
    parser.add_argument("--top-pct", type=float, default=5.0, help="Top percentile to write as outlier lists (default 5)")
    args = parser.parse_args()
    run(use_fulltext=args.fulltext, top_pct=args.top_pct)


if __name__ == "__main__":
    main()
