"""
Interdisciplinary collaboration network: nodes = publishers (departments),
edges = shared topic clusters. Edge weight = shared cluster count and/or cosine similarity.
Outputs: publisher_topic_network.csv, network plot, centrality rankings.
"""
from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).resolve().parent / "results"
FIGURES_DIR = Path(__file__).resolve().parent / "figures"
COUNTS_PATH = RESULTS_DIR / "publisher_cluster_counts.csv"


def load_publisher_counts(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.set_index(df.columns[0])
    df.index = df.index.astype(str).str.strip()
    return df


def shared_cluster_count(row_i: np.ndarray, row_j: np.ndarray) -> int:
    return int(((row_i > 0) & (row_j > 0)).sum())


def run() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading %s", COUNTS_PATH)
    counts = load_publisher_counts(COUNTS_PATH)
    publishers = counts.index.tolist()
    n = len(publishers)
    X = counts.values.astype(np.float64)

    # Pairwise: shared clusters and cosine similarity of cluster distributions
    log.info("Computing pairwise shared clusters and cosine similarity...")
    shared = np.zeros((n, n))
    for i in range(n):
        for j in range(i + 1, n):
            s = shared_cluster_count(X[i], X[j])
            shared[i, j] = shared[j, i] = s
    cos_sim = cosine_similarity(X)
    np.fill_diagonal(cos_sim, 0)

    # Build graph: edges only where shared clusters > 0; weight = shared count (primary)
    G = nx.Graph()
    G.add_nodes_from(publishers)
    for i in range(n):
        for j in range(i + 1, n):
            if shared[i, j] > 0:
                G.add_edge(
                    publishers[i],
                    publishers[j],
                    weight=shared[i, j],
                    shared_clusters=int(shared[i, j]),
                    cosine_similarity=float(cos_sim[i, j]),
                )

    log.info("Graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())

    # Centralities (use edge weight for degree / betweenness)
    degree_c = nx.degree_centrality(G)
    betweenness_c = nx.betweenness_centrality(G, weight="weight", seed=42)
    try:
        communities = nx.community.louvain_communities(G, weight="weight", seed=42)
    except TypeError:
        communities = nx.community.louvain_communities(G, seed=42)
    node_to_comm = {}
    for cid, comm in enumerate(communities):
        for node in comm:
            node_to_comm[node] = cid

    # Edges CSV
    edges_data = []
    for u, v, d in G.edges(data=True):
        edges_data.append({
            "source": u,
            "target": v,
            "shared_clusters": d["shared_clusters"],
            "cosine_similarity": round(d["cosine_similarity"], 6),
        })
    edges_df = pd.DataFrame(edges_data)
    edges_df = edges_df.sort_values("shared_clusters", ascending=False)
    network_path = RESULTS_DIR / "publisher_topic_network.csv"
    edges_df.to_csv(network_path, index=False)
    log.info("Saved %s", network_path)

    # Node metrics and ranked list
    nodes_df = pd.DataFrame({
        "publisher": publishers,
        "degree_centrality": [degree_c.get(p, 0) for p in publishers],
        "betweenness_centrality": [betweenness_c.get(p, 0) for p in publishers],
        "community_id": [node_to_comm.get(p, -1) for p in publishers],
    })
    nodes_df = nodes_df.sort_values("betweenness_centrality", ascending=False).reset_index(drop=True)
    nodes_df["rank_by_betweenness"] = nodes_df.index + 1
    nodes_df = nodes_df.sort_values("degree_centrality", ascending=False).reset_index(drop=True)
    nodes_df["rank_by_degree"] = nodes_df.index + 1
    # Final order: rank by betweenness (hub measure)
    nodes_df = nodes_df.sort_values("betweenness_centrality", ascending=False).reset_index(drop=True)
    rankings_path = RESULTS_DIR / "publisher_centrality_rankings.csv"
    nodes_df.to_csv(rankings_path, index=False)
    log.info("Saved %s", rankings_path)

    # Visualization
    fig, ax = plt.subplots(figsize=(12, 10))
    pos = nx.spring_layout(G, k=1.5, seed=42, iterations=50)
    weights = [G.edges[u, v]["weight"] for u, v in G.edges()]
    w_min, w_max = min(weights), max(weights)
    width = [2 + 2 * (w - w_min) / (w_max - w_min + 1e-9) for w in weights]
    nx.draw_networkx_edges(G, pos, width=width, alpha=0.5, ax=ax)
    node_colors = [node_to_comm.get(n, -1) for n in G.nodes()]
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, cmap="tab20", node_size=300, ax=ax)
    nx.draw_networkx_labels(G, pos, font_size=8, ax=ax)
    ax.set_title("Publisher topic network (edges = shared clusters, color = community)")
    ax.axis("off")
    plt.tight_layout()
    fig_path = FIGURES_DIR / "publisher_topic_network.png"
    plt.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved %s", fig_path)

    # Print top hubs
    log.info("Top publishers by betweenness (interdisciplinary hubs):")
    for _, row in nodes_df.head(10).iterrows():
        log.info("  %s  betweenness=%.4f  degree_cent=%.4f  community=%s",
                 row["publisher"], row["betweenness_centrality"], row["degree_centrality"], row["community_id"])


if __name__ == "__main__":
    run()
