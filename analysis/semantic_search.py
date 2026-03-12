"""
Semantic search: embed a query and return the top-k most similar theses (by cosine similarity).

Requires: thesis_semantic_clustering.py run first (embeddings cache). Uses same model (all-MiniLM-L6-v2).

Run: python analysis/semantic_search.py "optimization in transportation networks" [--top-k 20] [--fulltext]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESULTS_DIR = Path("analysis/results")
CACHE_DIR = Path("analysis/cache")
DATA_PATH = Path("Thesis_meta/thesis_meta_combined_filtered.csv")
CLUSTERING_LABELS_PATH = RESULTS_DIR / "clustering_labels.csv"
EMBEDDINGS_PATH_ABSTRACT = CACHE_DIR / "abstract_embeddings.npy"
EMBEDDINGS_PATH_FULLTEXT = CACHE_DIR / "fulltext_embeddings.npy"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


def load_embeddings_and_meta(use_fulltext: bool = False) -> tuple[np.ndarray, pd.DataFrame, pd.DataFrame | None]:
    labels_path = CLUSTERING_LABELS_PATH
    if not labels_path.exists():
        raise FileNotFoundError(f"Run thesis_semantic_clustering.py first. Missing {labels_path}")
    labels_df = pd.read_csv(labels_path, sep=",", encoding="utf-8", low_memory=False)
    emb_path = EMBEDDINGS_PATH_FULLTEXT if use_fulltext else EMBEDDINGS_PATH_ABSTRACT
    if not emb_path.exists():
        raise FileNotFoundError(f"Missing embeddings: {emb_path}")
    emb = np.load(emb_path)
    if len(labels_df) != len(emb):
        raise ValueError(f"Row count mismatch: labels {len(labels_df)} vs embeddings {len(emb)}")
    meta = None
    if DATA_PATH.exists():
        meta = pd.read_csv(DATA_PATH, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
        id_col = labels_df.columns[0]
        meta_id = id_col if id_col in meta.columns else meta.columns[0]
        abstract_col = None
        for c in meta.columns:
            if "abstract" in str(c).lower():
                abstract_col = c
                break
        if abstract_col:
            meta = meta[[meta_id, abstract_col]].drop_duplicates(subset=[meta_id])
        else:
            meta = meta[[meta_id]]
    return emb, labels_df, meta


def run(query: str, top_k: int = 20, use_fulltext: bool = False) -> None:
    from sentence_transformers import SentenceTransformer

    emb, labels_df, meta = load_embeddings_and_meta(use_fulltext=use_fulltext)
    id_col = labels_df.columns[0]
    ids = labels_df[id_col].astype(str).values

    model = SentenceTransformer(EMBEDDING_MODEL)
    q_emb = model.encode([query], normalize_embeddings=True)
    sim = cosine_similarity(q_emb, emb)[0]
    order = np.argsort(-sim)[:top_k]

    print(f"Query: {query}\nTop-{top_k} theses:\n")
    for r, i in enumerate(order, 1):
        tid = ids[i]
        score = sim[i]
        row = labels_df.iloc[i]
        pub = row.iloc[1] if len(row) > 1 else ""
        snippet = ""
        if meta is not None:
            m = meta[meta[meta.columns[0]].astype(str) == tid]
            if len(m) > 0 and len(meta.columns) > 1:
                snippet = str(m.iloc[0, 1])[:200] + "..." if len(str(m.iloc[0, 1])) > 200 else str(m.iloc[0, 1])
        print(f"  {r}. [{tid}] (score={score:.3f}) {pub}")
        if snippet:
            print(f"      {snippet}")
        print()
    results_df = pd.DataFrame({
        "rank": range(1, top_k + 1),
        "thesis_id": ids[order],
        "cosine_similarity": np.round(sim[order], 4),
        "publisher": [labels_df.iloc[j].iloc[1] if labels_df.shape[1] > 1 else "" for j in order],
    })
    out_path = RESULTS_DIR / "semantic_search_results.csv"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(out_path, index=False)
    log.info("Saved %s", out_path)


def main():
    parser = argparse.ArgumentParser(description="Semantic search over thesis embeddings")
    parser.add_argument("query", type=str, help="Search query (e.g. 'optimization in transportation networks')")
    parser.add_argument("--top-k", type=int, default=20, help="Number of results (default 20)")
    parser.add_argument("--fulltext", action="store_true", help="Use fulltext embeddings cache")
    args = parser.parse_args()
    run(query=args.query, top_k=args.top_k, use_fulltext=args.fulltext)


if __name__ == "__main__":
    main()
