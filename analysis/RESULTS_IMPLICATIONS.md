# Thesis semantic clustering: results and implications

This document explains what the analysis did and how to interpret the outputs.

---

## Overview of analyses and outputs

There are **three analysis pipelines**. Run them in order when using faculty-level outputs.

| Pipeline | Script | Purpose |
|----------|--------|--------|
| **1. Thesis semantic clustering** | `analysis/thesis_semantic_clustering.py` | Embed abstracts (or full text from GCS), cluster theses by topic (HDBSCAN), produce 2D views and per-cluster keywords. Writes **clustering_labels.csv** and embeddings cache for pipeline 3. |
| **2. Faculty writing analysis** | `analysis/faculty_writing_analysis.py` | Compare how faculties write: text stats (word count, sentence length, TTR) and distinctive vocabulary per faculty. Does not use clustering. |
| **3. Faculty clustering analysis** | `analysis/faculty_clustering_analysis.py` | Uses **clustering_labels.csv** and the embeddings cache from pipeline 1. Produces faculty-level topic concentration, similarity, affinity to clusters, exemplar theses, temporal trends, and keyword usage. |

**Output locations**

- **CSVs:** `analysis/results/`
- **Figures:** `analysis/figures/`
- **Embeddings (saved to disk):** `analysis/cache/abstract_embeddings.npy` and `analysis/cache/fulltext_embeddings.npy`. Pipeline 1 writes these files and loads from them only when needed (UMAP, then t-SNE, then PCA), so the full embedding matrix is not kept in memory for the whole run.
- **Full-text data (GCS):** When using `--fulltext-gcs`, downloaded full text and the filtered dataframe are cached in `analysis/cache/fulltext_data.pkl`. Re-runs skip the GCS download and use this cache (and the embeddings cache) so only UMAP/t-SNE/PCA and figures run. Delete `fulltext_data.pkl` to force a fresh download (e.g. after changing `--data` or bucket/prefix).

**Quick output reference**

| Output | Pipeline | Description |
|--------|----------|-------------|
| clustering_labels.csv | 1 | Per-thesis: id, Publisher, cluster_id [, year]. |
| cluster_keywords.csv | 1 | Per cluster: size, top 15 keywords. |
| publisher_topic_distribution.csv | 1 | Publisher × cluster normalized shares. |
| publisher_cluster_counts.csv | 1 | Publisher × cluster counts. |
| interdisciplinary_clusters.csv | 1 | Per cluster: publisher count and entropy. |
| embedding_umap_clusters.png | 1 | 2D UMAP, color = cluster. |
| embedding_umap_faculty.png | 1 | 2D UMAP, color = faculty (Publisher); legend for semantics. |
| embedding_umap_interdisciplinary.png | 1 | 2D UMAP, color = cluster entropy. |
| embedding_tsne_clusters.png, embedding_tsne_faculty.png | 1 | 2D t-SNE by cluster / by faculty. |
| embedding_pca_clusters.png, embedding_pca_faculty.png | 1 | 2D PCA by cluster / by faculty. |
| publisher_topic_heatmap.png | 1 | Rows = publishers, columns = clusters. |
| cluster_size_distribution.png | 1 | Bar chart of cluster sizes. |
| faculty_writing_stats.csv | 2 | Per faculty: n_theses, mean word count, sentence stats, TTR. |
| faculty_distinctive_terms.csv | 2 | Per faculty: top 20 TF-IDF terms. |
| faculty_mean_words_per_sentence.png, faculty_type_token_ratio.png | 2 | Bar charts by faculty. |
| faculty_topic_concentration_noise.csv | 3 | Per faculty: n_theses, noise_share, topic_entropy, herfindahl. |
| faculty_topic_similarity.csv + faculty_topic_similarity_heatmap.png | 3 | Faculty–faculty correlation (topic profile). |
| faculty_affinity_to_clusters.csv | 3 | Per faculty: distance to each cluster centroid. |
| faculty_cluster_exemplars.csv | 3 | Exemplar theses per faculty per cluster. |
| faculty_year_topic_distribution.csv | 3 | (If year in labels) Faculty × year × cluster shares. |
| faculty_keyword_usage.csv | 3 | Per faculty: TF-IDF sums for a fixed keyword list. |
| faculty_topic_diversity_noise.png | 3 | Bar charts: topic entropy and noise share by faculty. |
| nearest_neighbors.csv | NN | Per-thesis top-k nearest neighbors (thesis_id, rank, neighbor_id, cosine_similarity). |
| department_nn_similarity_stats.csv | NN | Per-department mean/median similarity to neighbors. |
| outlier_scores.csv | Out | Per-thesis: dist_to_centroid, LOF score, Isolation Forest anomaly. |
| outliers_top_lof.csv, outliers_top_isolation_forest.csv | Out | Top outlier theses by LOF / Isolation Forest. |
| similarity_graph_nodes.csv | Graph | Per-thesis: degree, PageRank, community_id. |
| similarity_graph_community_sizes.csv | Graph | Community size distribution. |
| semantic_search_results.csv | Search | Top-k theses for a query (from `semantic_search.py`). |

**Additional scripts (run after pipeline 1):** `nearest_neighbor_analysis.py` (NN), `outlier_detection.py` (Out), `similarity_graph_analysis.py` (Graph), `semantic_search.py` (Search). Each reads `clustering_labels.csv` and the embeddings cache.

---

## How to run and how long it takes

**Prerequisites:** Thesis meta CSV at `Thesis_meta/thesis_meta_combined_filtered.csv` (or pass `--data path/to/file.csv`). For full-text runs, GCS credentials and access to the markdown bucket. From the project root:

```bash
# Pipeline 1 – thesis semantic clustering (required first for 2 & 3)
uv run python analysis/thesis_semantic_clustering.py

# Optional: subsample for a quick test (e.g. 500 theses)
uv run python analysis/thesis_semantic_clustering.py --sample-size 500

# Optional: use full text from GCS (slower: download + chunked embedding)
uv run python analysis/thesis_semantic_clustering.py --fulltext-gcs

# Optional: use Department_new from GCS for faculty (more accurate than Publisher)
uv run python analysis/thesis_semantic_clustering.py --department-gcs
# Department CSV default: thesis_archive_bucket/dtu_findit/master_thesis_meta/thesis_meta_combined_department.csv

# Optional: recompute embeddings (ignore cache)
uv run python analysis/thesis_semantic_clustering.py --recompute-embeddings
```

```bash
# Pipeline 2 – faculty writing (independent; no clustering needed)
uv run python analysis/faculty_writing_analysis.py

# Full text from GCS
uv run python analysis/faculty_writing_analysis.py --fulltext-gcs

# Quick test on a sample
uv run python analysis/faculty_writing_analysis.py --sample-size 500
```

```bash
# Pipeline 3 – faculty clustering (uses pipeline 1 outputs)
uv run python analysis/faculty_clustering_analysis.py

# If you used fulltext in pipeline 1
uv run python analysis/faculty_clustering_analysis.py --fulltext
```

```bash
# Nearest-neighbor analysis (top-k per thesis, department similarity stats)
uv run python analysis/nearest_neighbor_analysis.py [--fulltext] [--top-k 10]

# Outlier detection (LOF, Isolation Forest, distance-to-centroid)
uv run python analysis/outlier_detection.py [--fulltext] [--top-pct 5]

# Similarity graph (edges where cosine > threshold; degree, PageRank, communities)
uv run python analysis/similarity_graph_analysis.py [--fulltext] [--threshold 0.5]

# Semantic search (query → top-k theses)
uv run python analysis/semantic_search.py "your query here" [--top-k 20] [--fulltext]
```

**Rough runtimes** (~6,000 theses, typical laptop):

| Pipeline | Abstracts only | Full text (GCS) |
|----------|----------------|-----------------|
| **1. Thesis semantic clustering** | **~5–15 min** (embedding ~2–5 min if uncached, UMAP/t-SNE/PCA ~2–8 min, rest & I/O &lt;1 min). With cache: **~3–8 min** (skip embedding). | **~30–90+ min** (GCS download + chunk-embedding; depends on network and chunk size). |
| **2. Faculty writing** | **~1–3 min** (TF-IDF and stats over abstracts). | **~20–60+ min** with `--fulltext-gcs` (download + stats). |
| **3. Faculty clustering** | **~1–2 min** (reads labels + cache; no heavy embedding). | Same (uses existing fulltext cache from pipeline 1). |

So: **abstracts-only full run (1 → 2 → 3) is about 10–20 minutes** if pipeline 1 uses cached embeddings; **full-text pipeline 1** is the main cost (tens of minutes to over an hour). Pipeline 3 is always short once pipeline 1 has been run.

---

## What was done

- **Data:** ~6,000 DTU master theses (with abstracts) from `thesis_meta_combined_filtered.csv`, linked to **Publisher** (DTU department/section).
- **Embeddings:** Abstract text was embedded with SentenceTransformer (`all-MiniLM-L6-v2`).
- **Clustering:** HDBSCAN was used to find topic clusters without fixing the number of clusters in advance. Some points are left as **noise** (cluster `-1`).
- **2D views:** UMAP, t-SNE, and PCA were used to project theses into 2D for visualization.
- **Interpretation:** For each cluster, TF-IDF over abstracts gave **top keywords**. Per cluster we also computed **publisher (section) distribution** and **interdisciplinary score** (publisher entropy).

---

## 1. Dominant research themes

The **cluster_keywords.csv** file lists, for each cluster, its size and top 15 keywords. From that:

- **Large, coherent topic clusters** (examples):
  - **Wind energy** (e.g. cluster 32: wind, turbine, wake, blade, floating; cluster 28: wind farm, optimization, cable, AEP).
  - **Energy systems** (cluster 98: hydrogen, grid, renewable, storage, BESS, market; cluster 90: heat, solar, district heating, thermal).
  - **Machine learning / NLP** (cluster 63: LLMs, language, NLP, document; cluster 16: anomaly, neural, forecasts, detection).
  - **Process mining / business** (cluster 17: process mining, business, distributed, monitoring).
  - **Buildings & indoor** (cluster 102: energy, building, indoor, ventilation, thermal).
  - **Health & biomedical** (e.g. cluster 67: drug, mRNA, delivery, liposomes; cluster 76: CAR, immune, cancer, MHC).
  - **Aquatic / fisheries** (cluster 25: species, fish, mackerel, climate, larvae; National Institute of Aquatic Resources is strong here).
  - **Electrolysis / hydrogen** (cluster 21: cell, electrolysis, electrodes, hydrogen).
  - **Computer vision** (cluster 107: detection, object, vision, camera; cluster 108: segmentation, image, brain).

- **Noise cluster (cluster -1):** ~1,150 theses. Keywords are generic (model, data, design, study, etc.). These are theses that did not fall into a clear topic cluster—either broad/interdisciplinary, or abstract text too short/generic.

- **Artifact clusters:** A few small clusters (e.g. 0, 1, 2, 3) have keywords like "timeout", "googleapis", "error"—likely theses whose "abstract" is actually an error message or placeholder. You can exclude these when interpreting themes.

**Implication:** The dominant themes in the dataset are **wind/renewable energy**, **ML/AI and data**, **health/biomedical**, **buildings and energy**, and **materials/chemistry**. The noise cluster is a mix of hard-to-classify or generic theses.

---

## 2. Which sections focus on which topics

The **publisher_topic_distribution.csv** (and **publisher_cluster_counts.csv**) give, for each DTU section (Publisher), the **share of that section's theses** (or counts) in each cluster. So you see:

- **Section specialization:**
  - **Wind and Energy Systems:** High share in wind-related clusters (e.g. 32, 28, 15, 13).
  - **National Food Institute:** Strong in food, fermentation, microbiology-style clusters (e.g. 53, 54, 70, 71).
  - **Applied Mathematics and Computer Science:** Spread across many clusters (ML, process mining, algorithms, verification, security).
  - **Health Technology:** Concentrated in health, imaging, rehabilitation, user/patient studies.
  - **Civil and Mechanical Engineering:** Structural, fatigue, materials, wind (e.g. 38, 39, 26, 32).
  - **Space Research and Space Technology:** Space, antenna, cubesat, detector, ray (e.g. 7, 11, 20).
  - **National Institute of Aquatic Resources:** Very strong in fish/species/ecosystem (e.g. cluster 25).
  - **Technology, Management and Economics:** Optimization, market, strategy, management, sustainability (e.g. 33, 73, 78, 92, 122).

- **Heatmap:**
  `publisher_topic_heatmap.png` shows **rows = publishers**, **columns = clusters**, **color = normalized share** of that publisher's theses in that cluster. Darker = more focused. Rows with one or two dark cells are highly specialized; rows with many light cells are more spread across topics.

**Implication:** Sections do specialize (wind, food, aquatic, space, health, etc.), but several sections (e.g. Applied Math/CS, Civil/Mechanical) appear in many clusters, which reflects both breadth and overlap in themes (e.g. wind, ML, optimization).

---

## 3. Interdisciplinary clusters

**interdisciplinary_clusters.csv** lists each cluster with:

- **number_of_publishers:** How many different sections have theses in that cluster.
- **publisher_entropy:** How evenly theses are spread across those sections (higher = more even mix).

**Findings:**

- **Cluster -1 (noise)** has the highest entropy and many publishers: it is a "catch-all" for theses that don't sit in a clear topic, and it is naturally cross-sectional.
- **Clusters 1, 2, 3** also have high entropy and many publishers—but from keywords they look like artifact/error-message clusters; treat with caution.
- Among **meaningful clusters**, those with relatively high publisher count and entropy are the most **interdisciplinary** (e.g. some energy, ML, or process/data clusters where several departments contribute).

**Figure:** `embedding_umap_interdisciplinary.png` colors each point by its **cluster's publisher entropy**. Yellow (high entropy) = more interdisciplinary clusters; purple (low) = section-specific clusters.

**Implication:** True interdisciplinary bridges are best read from clusters that both have interpretable keywords (e.g. energy, ML, sustainability) and high publisher entropy. The UMAP interdisciplinary plot helps spot regions of the thesis landscape where many sections meet.

---

## 4. How to read the figures

| Figure | What it shows |
|--------|----------------|
| **embedding_umap_clusters.png** | 2D UMAP of theses; **color = cluster**. Same cluster = same topic; nearby points = similar abstracts. |
| **embedding_umap_faculty.png** | Same UMAP; **color = faculty** (Publisher). See which departments occupy which semantic regions. |
| **embedding_umap_interdisciplinary.png** | Same UMAP; **color = interdisciplinary** (cluster's publisher entropy). High = many sections in that topic. |
| **embedding_tsne_clusters.png** / **embedding_tsne_faculty.png** | 2D t-SNE; **color = cluster** or **faculty**. |
| **embedding_pca_clusters.png** / **embedding_pca_faculty.png** | 2D PCA; **color = cluster** or **faculty**. |
| **publisher_topic_heatmap.png** | Rows = sections, columns = clusters; **cell = share** of that section's theses in that cluster. |
| **cluster_size_distribution.png** | Bar chart of cluster sizes. Highlights dominant and small clusters. |

---

## 5. Caveats

- **Noise cluster (-1):** Large and generic; not a single "theme" but theses that don't fit clear topics.
- **A few clusters** (0, 1, 2, 3) look like **bad/placeholder abstracts** (errors, timeouts); exclude when describing research themes.
- **Publisher** in the data is "issuing department/section", not supervisor; it still reflects where the thesis is anchored.
- **Abstracts only:** Themes are from abstract text. Full-text would add depth and might change cluster boundaries.

---

## 6. Short answers to the original questions

- **What are the dominant research themes?**
  Wind/renewable energy, ML/AI and data science, health/biomedical, buildings and energy, process/mining and business, materials/chemistry, and aquatic/climate. See **cluster_keywords.csv** and **cluster_size_distribution.png**.

- **Which sections focus on which topics?**
  See **publisher_topic_distribution.csv** and **publisher_topic_heatmap.png**. Sections have clear peaks (e.g. Wind, Food, Aquatic, Space, Health) and some (e.g. Applied Math/CS, Management) span many clusters.

- **Are there interdisciplinary clusters?**
  Yes. **interdisciplinary_clusters.csv** and **embedding_umap_interdisciplinary.png** identify clusters with many sections and high entropy. Exclude noise (-1) and artifact clusters when naming "bridges"; the rest are candidates for cross-department themes (e.g. energy, ML, sustainability).

---

## 7. Faculty writing analysis: method

A separate analysis compares **how different faculties (publishers) write** their theses—stylistic and lexical differences rather than topic. Script: `analysis/faculty_writing_analysis.py`.

### Data

- Same meta as clustering: `thesis_meta_combined_filtered.csv` with **Publisher** (faculty/department) and **abstract_ts** (or full text from GCS markdown with `--fulltext-gcs`).
- Rows with missing or very short text (< 50 characters) are dropped. Each thesis is assigned to one faculty via the Publisher column.

### Text statistics (per faculty)

For each thesis we compute simple text metrics; then we aggregate **per faculty** (mean or median across its theses):

- **Word count** – total tokens (words) per document (abstract or full text).
- **Sentence count** – sentences detected by splitting on `.`, `!`, `?`.
- **Words per sentence** – mean sentence length (proxy for syntactic complexity).
- **Type–token ratio (TTR)** – unique words / total words, per document, then averaged. Higher TTR = more varied vocabulary; lower can indicate repetition or more formulaic/technical prose.

All measures are computed on the same text source (abstracts or full text) for the run.

### Distinctive vocabulary

To see **which terms are characteristic of each faculty** (rather than topic):

- One **document per faculty**: all theses from that faculty are concatenated into a single text.
- **TF-IDF** is run over this set of faculty-documents (each faculty = one "document"). This down-weights terms that appear in many faculties and highlights terms that are relatively specific to one (or a few) faculties.
- For each faculty we keep the **top 20 terms** by TF-IDF weight and write them to `faculty_distinctive_terms.csv` (pipe-separated).

So the distinctive-terms table is "how this faculty's writing is lexically marked" relative to the others, mixing topic and style.

### Outputs

| Output | Content |
|--------|--------|
| **faculty_writing_stats.csv** | One row per faculty: n_theses, mean/median word count, mean sentence count, mean words per sentence, mean type–token ratio. |
| **faculty_distinctive_terms.csv** | One row per faculty: top 20 TF-IDF terms (pipe-separated). |
| **faculty_mean_words_per_sentence.png** | Horizontal bar chart: mean sentence length by faculty. |
| **faculty_type_token_ratio.png** | Horizontal bar chart: mean vocabulary diversity (TTR) by faculty. |

### How to interpret

- **Sentence length:** Higher mean words per sentence suggests longer, more complex sentences on average; lower suggests shorter, simpler sentences.
- **Type–token ratio:** Higher = more lexical variety; lower = more repetition or more constrained vocabulary (e.g. technical or formulaic writing).
- **Distinctive terms:** Compare rows across faculties to see which words (and thus topics/jargon) are relatively specific to each. Not a causal measure—faculties differ in both discipline and writing norms.

### Run

```bash
# Abstracts only
uv run python analysis/faculty_writing_analysis.py

# Full-text from GCS markdown
uv run python analysis/faculty_writing_analysis.py --fulltext-gcs
```

---

## 8. Faculty clustering analysis: outputs

Pipeline 3 (`analysis/faculty_clustering_analysis.py`) uses the clustering labels and embeddings from pipeline 1 to produce **faculty-level** topic and keyword summaries.

**Prerequisite:** Run `thesis_semantic_clustering.py` first so that `analysis/results/clustering_labels.csv` and `analysis/cache/abstract_embeddings.npy` (or `fulltext_embeddings.npy` with `--fulltext`) exist.

### Outputs

| Output | Content |
|--------|--------|
| **faculty_topic_concentration_noise.csv** | One row per faculty: n_theses, noise_count, noise_share (fraction in cluster -1), topic_entropy (higher = more diverse topics), topic_concentration_herfindahl (higher = more concentrated). |
| **faculty_topic_similarity.csv** | Matrix of faculty–faculty correlations: each faculty’s topic distribution (share per cluster) is turned into a vector; cells are correlation between those vectors. Similar faculties have similar topic mixes. |
| **faculty_topic_similarity_heatmap.png** | Heatmap of the above matrix (RdBu_r, center 0). |
| **faculty_affinity_to_clusters.csv** | One row per faculty: columns are cluster_0, cluster_1, … with the **distance** from that faculty’s centroid (in embedding space) to each cluster centroid. Lower = closer to that topic. |
| **faculty_cluster_exemplars.csv** | For each faculty and each cluster (where that faculty has theses): up to 2 “exemplar” theses (closest to the faculty–cluster centroid in embedding space), with thesis id and short abstract snippet. |
| **faculty_year_topic_distribution.csv** | (Only if labels include a year column.) Per faculty, per year, per cluster: normalized share of that faculty’s theses in that cluster. For temporal trends. |
| **faculty_keyword_usage.csv** | Per faculty: TF-IDF sum over abstracts for a fixed keyword list (e.g. wind, turbine, learning, hydrogen, …). One row per faculty–keyword; columns include n_docs, tfidf_sum. |
| **faculty_topic_diversity_noise.png** | Two horizontal bar charts: (1) topic entropy by faculty, (2) noise_share by faculty. |

### How to interpret

- **Topic concentration / entropy:** Faculties with high entropy spread theses across many clusters; low entropy means a few clusters dominate (more focused).
- **Noise share:** High noise_share means many of that faculty’s theses fell in cluster -1 (no clear topic).
- **Similarity heatmap:** Faculties that appear in similar clusters will show high correlation; distinct specializations show low or negative correlation.
- **Affinity to clusters:** Use to see “which topics is this faculty closest to?” in embedding space, not just count-based shares.
- **Exemplars:** Quick way to attach concrete thesis IDs and snippets to each faculty–cluster cell.
- **Keyword usage:** Compare how much each faculty’s abstracts use the predefined terms (e.g. “wind”, “learning”); relative differences, not absolute.

### Run

```bash
# Use abstract embeddings (default)
uv run python analysis/faculty_clustering_analysis.py

# Use fulltext embeddings cache from pipeline 1
uv run python analysis/faculty_clustering_analysis.py --fulltext

# Custom thesis meta path (for exemplars and keyword usage)
uv run python analysis/faculty_clustering_analysis.py --data path/to/thesis_meta.csv
```

---

## 9. Full-text run: interpretation

The following summarizes a **full-text** clustering run (GCS markdown → chunk-and-average embeddings → HDBSCAN). ~6,215 theses with full text; 118 clusters (including noise -1).

### Scale and noise

- **Noise (cluster -1):** 1,256 theses (~20%). Top keywords: cid, figure, model, data, 10, used, time, results, energy, et, al, analysis. As with abstract-only runs, this is the catch-all for broad or generic theses; full text adds more “figure/model/data” from methods sections.
- **Largest topic clusters (by size):** 4 (323 – wind/turbine/wake/blade), 14 (193 – security/devices/5G/IoT/blockchain), 35 (122 – neural/learning/training), 100 (125 – energy/market/capacity/battery/grid), 23 (117 – heat/solar/storage/district heating), 114 (160 – project/design/sustainability/management/business), 56 (85 – building/ventilation/thermal), 45 (83 – concrete/soil/cement), 52 (79 – robot/control/learning), 92 (72 – power/grid/converter/wind/HVDC), 68 (95 – language/text/dataset/learning).

### Themes (full text vs abstracts)

- **Wind & energy:** Same as abstracts (clusters 4, 92, 99, 100, 69 blade/design). Full text adds more methods vocabulary (figure, model, 10, cid).
- **ML / data / security:** 14 (IoT, 5G, attestation, blockchain), 35 (neural, training), 42 (EEG/sleep), 68 (language/text), 95 (detection/images). More technical and methods-heavy than abstract-only.
- **Health & bio:** 79 (CAR, cancer, MHC, immune), 12 (cells, printing, drug, liposomes), 97 (single-cell, genes), 98 (sequencing). Full text surfaces methods (doi, figure, 10).
- **Buildings & energy systems:** 56 (building, ventilation, thermal), 23 (heat, solar, district heating), 49 (indoor, air, CO2). Aligns with abstract themes.
- **Aquatic / food:** 5 (fish, mackerel, species, marine), 90 (fermentation, glutamicum), 93 (food, protein, emulsions). National Food Institute and Aquatic Resources remain clearly visible in publisher distribution.
- **Danish-language cluster:** 1 (52 theses): top terms og, af, er, det, til, der, på, med, som, den, ikke, fra. Theses written (or with full text) in Danish form a distinct cluster.
- **Artifacts / weak clusters:** 0, 7, 9 have numeric/cid-heavy keywords (e.g. 72, 87, 76, 10, 00, 000). Likely bad or placeholder full text; treat like abstract artifact clusters.

### Interdisciplinary (full-text)

- **Noise (-1)** again has the highest publisher entropy (2.56) and many publishers (19).
- **High entropy among content clusters:** 9, 7, 88 (hydrogen/energy), 35 (ML), 23 (heat/solar), 114 (management/sustainability). So energy, ML, and management/sustainability remain the main cross-department themes in full text as well.

### Takeaways

- Full-text clustering **recovers the same broad themes** as abstract-only (wind, ML, health, buildings, energy, aquatic, management) but with **more and smaller clusters** and more methods/technical terms (figure, model, cid, 10).
- **Noise share (~20%)** is similar to abstract runs; full text does not dramatically reduce it.
- **Danish cluster (1)** is new relative to abstract-only and useful for language/audience analysis.
- **Section specialization** in `publisher_topic_distribution.csv` matches the abstract run: Wind, Food, Aquatic, Health Tech, Applied Math/CS, etc. have the same “home” clusters; full text refines within those areas.
- For **pipeline 3** (faculty clustering analysis), run with `--fulltext` so it uses `fulltext_embeddings.npy` and the labels from this run; concentration, similarity, and exemplars will then reflect full-text topic structure.
