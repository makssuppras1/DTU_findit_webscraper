"""
Similarity graph: nodes = theses, edges where cosine similarity > threshold.
Output: degree, PageRank, community ID per thesis.

Requires: thesis_semantic_clustering.py run first.

Run: python analysis/similarity_graph_analysis.py [--fulltext] [--threshold 0.5]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
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


def run(use_fulltext: bool = False, threshold: float = 0.5, chunk_size: int = 1000) -> None:
    import networkx as nx

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    labels_df, embeddings = load_labels_and_embeddings(use_fulltext=use_fulltext)
    n = len(embeddings)
    id_col = labels_df.columns[0]
    ids = labels_df[id_col].astype(str).values

    # Build sparse adjacency: for each row, compute similarity to others in chunks to limit memory
    log.info("Building similarity graph (threshold=%.2f)...", threshold)
    rows, cols, data = [], [], []
    for i in tqdm(range(0, n, chunk_size), desc="Similarity chunks"):
        end = min(i + chunk_size, n)
        block = embeddings[i:end]
        sim = cosine_similarity(block, embeddings)
        np.fill_diagonal(sim, 0)
        for ii, i_glob in enumerate(range(i, end)):
            for j in range(n):
                if i_glob != j and sim[ii, j] >= threshold:
                    rows.append(i_glob)
                    cols.append(j)
                    data.append(float(sim[ii, j]))
    A = csr_matrix((data, (rows, cols)), shape=(n, n))
    # Symmetrize so undirected
    A = (A + A.T).maximum(A)

    try:
        G = nx.from_scipy_sparse_array(A)
    except AttributeError:
        G = nx.from_scipy_sparse_matrix(A)
    log.info("Graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())

    degree = dict(G.degree())
    log.info("Computing PageRank...")
    pagerank = nx.pagerank(G)
    log.info("Computing communities (Louvain)...")
    try:
        communities = nx.community.louvain_communities(G, seed=42)
    except AttributeError:
        communities = list(nx.community.greedy_modularity_communities(G))
    node_to_comm = {}
    for cid, comm in enumerate(communities):
        for node in comm:
            node_to_comm[node] = cid

    out_df = pd.DataFrame({
        "thesis_id": ids,
        "degree": [degree.get(i, 0) for i in range(n)],
        "pagerank": [pagerank.get(i, 0.0) for i in range(n)],
        "community_id": [node_to_comm.get(i, -1) for i in range(n)],
    })
    out_df.to_csv(RESULTS_DIR / "similarity_graph_nodes.csv", index=False)
    log.info("Saved similarity_graph_nodes.csv")

    comm_sizes = pd.Series([node_to_comm.get(i, -1) for i in range(n)]).value_counts()
    comm_sizes.to_csv(RESULTS_DIR / "similarity_graph_community_sizes.csv", header=["count"])
    log.info("Saved similarity_graph_community_sizes.csv")


def main():
    parser = argparse.ArgumentParser(description="Similarity graph analysis")
    parser.add_argument("--fulltext", action="store_true", help="Use fulltext embeddings cache")
    parser.add_argument("--threshold", type=float, default=0.5, help="Min cosine similarity for an edge (default 0.5)")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Chunk size for similarity matrix (reduce if OOM)")
    args = parser.parse_args()
    run(use_fulltext=args.fulltext, threshold=args.threshold, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
