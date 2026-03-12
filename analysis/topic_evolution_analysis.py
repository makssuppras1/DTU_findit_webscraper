"""
Topic evolution over time: cluster share per year, growth rates, emerging/declining topics.

Input: clustering_labels.csv (thesis_id, cluster_id, Publisher, year).
Outputs: cluster_year_distribution.csv, cluster_growth_rates.csv, line plots, heatmap.

Run: python analysis/topic_evolution_analysis.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESULTS_DIR = Path("analysis/results")
FIGURES_DIR = Path("analysis/figures")
CLUSTERING_LABELS_PATH = RESULTS_DIR / "clustering_labels.csv"


def _detect_cols(df: pd.DataFrame) -> dict:
    out = {"id": df.columns[0], "publisher": df.columns[1], "cluster_id": "cluster_id"}
    if "cluster_id" not in df.columns:
        for c in df.columns:
            if "cluster" in str(c).lower():
                out["cluster_id"] = c
                break
    year_candidates = ["year", "Year", "Publication Year", "date", "graduation_year"]
    out["year"] = next((c for c in year_candidates if c in df.columns), None)
    return out


def run() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    if not CLUSTERING_LABELS_PATH.exists():
        raise FileNotFoundError(f"Missing {CLUSTERING_LABELS_PATH}. Run thesis_semantic_clustering.py first.")
    df = pd.read_csv(CLUSTERING_LABELS_PATH, sep=",", encoding="utf-8", low_memory=False)
    cols = _detect_cols(df)
    cluster_col = cols["cluster_id"]
    year_col = cols.get("year")
    if not year_col or year_col not in df.columns:
        raise ValueError("clustering_labels.csv must have a year column (year, Year, etc.)")

    df = df.dropna(subset=[year_col, cluster_col])
    df["year"] = pd.to_numeric(df[year_col], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df = df[df[cluster_col] != -1].copy()

    years = sorted(df["year"].unique())
    clusters = sorted(df[cluster_col].unique())
    log.info("Years: %s .. %s, %d clusters (excluding noise)", min(years), max(years), len(clusters))

    # Counts per cluster per year
    count_df = df.groupby([cluster_col, "year"]).size().reset_index(name="count")
    # Total theses per year (for normalization)
    total_per_year = df.groupby("year").size()
    count_df["total_in_year"] = count_df["year"].map(total_per_year)
    count_df["share"] = (count_df["count"] / count_df["total_in_year"]).round(4)
    count_df = count_df.drop(columns=["total_in_year"])
    count_df.to_csv(RESULTS_DIR / "cluster_year_distribution.csv", index=False)
    log.info("Saved cluster_year_distribution.csv")

    # Pivot for heatmap and growth: rows = cluster, cols = year, values = share
    share_pivot = count_df.pivot(index=cluster_col, columns="year", values="share").fillna(0)

    # Growth rates and emerging/declining
    if len(years) >= 2:
        first_year, last_year = min(years), max(years)
        share_first = share_pivot[first_year] if first_year in share_pivot.columns else share_pivot.iloc[:, 0]
        share_last = share_pivot[last_year] if last_year in share_pivot.columns else share_pivot.iloc[:, -1]
        share_change = share_last - share_first
        # Slope of share vs year (linear regression slope per cluster)
        slope = []
        for c in share_pivot.index:
            y_vals = np.array([share_pivot.loc[c].get(y, 0) for y in years])
            if y_vals.sum() == 0:
                slope.append(0.0)
            else:
                slope.append(float(np.polyfit(years, y_vals, 1)[0]))
        first_appearance = share_pivot.apply(lambda r: r[r > 0].index.min() if (r > 0).any() else np.nan, axis=1)
        last_appearance = share_pivot.apply(lambda r: r[r > 0].index.max() if (r > 0).any() else np.nan, axis=1)
        n_years_active = share_pivot.apply(lambda r: (r > 0).sum(), axis=1)
        only_recent = (last_year - first_appearance) <= 3
        growth_df = pd.DataFrame({
            "cluster_id": share_pivot.index,
            "share_first_year": share_first.values,
            "share_last_year": share_last.values,
            "share_change": share_change.values,
            "share_slope": np.round(slope, 6),
            "first_year_active": first_appearance.values,
            "last_year_active": last_appearance.values,
            "n_years_active": n_years_active.values,
            "only_recent_years": only_recent.values,
        }).reset_index(drop=True)
        growth_df = growth_df.sort_values("share_change", ascending=False)
        growth_df.to_csv(RESULTS_DIR / "cluster_growth_rates.csv", index=False)
        log.info("Saved cluster_growth_rates.csv")
    else:
        growth_df = None

    # Figures
    import matplotlib.pyplot as plt
    import seaborn as sns

    # Heatmap: cluster × year (share)
    fig, ax = plt.subplots(figsize=(max(8, len(years) * 0.4), max(6, len(clusters) * 0.25)))
    sns.heatmap(share_pivot, ax=ax, cmap="YlOrRd", cbar_kws={"label": "Share of theses in year"})
    ax.set_title("Cluster share by year (topic evolution)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Cluster")
    plt.xticks(rotation=45)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "cluster_year_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved cluster_year_heatmap.png")

    # Line plot: share over time for each cluster (top N by total activity to avoid clutter)
    cluster_totals = share_pivot.sum(axis=1)
    top_clusters = cluster_totals.nlargest(min(25, len(clusters))).index.tolist()
    plot_df = share_pivot.loc[top_clusters]
    fig, ax = plt.subplots(figsize=(10, 6))
    for c in plot_df.index:
        ax.plot(years, [plot_df.loc[c].get(y, 0) for y in years], label=f"Cluster {c}", alpha=0.8)
    ax.set_xlabel("Year")
    ax.set_ylabel("Share of theses in year")
    ax.set_title("Cluster share over time (top 25 clusters by activity)")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=6, ncol=1)
    fig.tight_layout(rect=[0, 0, 0.85, 1])
    fig.savefig(FIGURES_DIR / "cluster_share_over_time.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved cluster_share_over_time.png")

    # Line plot: emerging (largest positive share change) and declining (largest negative)
    if growth_df is not None and len(growth_df) >= 2:
        emerging = growth_df.nlargest(5, "share_change")["cluster_id"].tolist()
        declining = growth_df.nsmallest(5, "share_change")["cluster_id"].tolist()
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
        for c in emerging:
            row = share_pivot.loc[c]
            ax1.plot(years, [row.get(y, 0) for y in years], label=f"Cluster {c}", marker="o", markersize=3)
        ax1.set_ylabel("Share")
        ax1.set_title("Emerging topics (largest increase in share)")
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        for c in declining:
            row = share_pivot.loc[c]
            ax2.plot(years, [row.get(y, 0) for y in years], label=f"Cluster {c}", marker="o", markersize=3)
        ax2.set_xlabel("Year")
        ax2.set_ylabel("Share")
        ax2.set_title("Declining topics (largest decrease in share)")
        ax2.legend(fontsize=8)
        ax2.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "cluster_emerging_declining.png", dpi=150, bbox_inches="tight")
        plt.close()
        log.info("Saved cluster_emerging_declining.png")


def main():
    run()


if __name__ == "__main__":
    main()
