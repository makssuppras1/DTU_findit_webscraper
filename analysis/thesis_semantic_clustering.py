"""
Thesis semantic clustering: topic discovery and department–topic maps from DTU theses.

What it does:
  1. Data: Load thesis metadata CSV. Optionally load full-text markdown from GCS (use_fulltext_gcs);
     otherwise use the abstract column. Faculty = Department_new (from GCS or local department CSV).
  2. Embeddings: SentenceTransformer on abstracts (or chunked full text with title prepend and
     weighted intro/conclusion). L2-normalize. Cache per model (abstract_embeddings_<model>.npy).
  3. Reduction: UMAP 2D (plots) and 10D (for clustering). Optional t-SNE 2D (config: tsne_perplexities).
  4. Clustering: HDBSCAN (or kmeans/agg) on UMAP-10 or raw embeddings; metric=euclidean, leaf selection.
  5. Keywords: TF-IDF fit on all docs once, top terms per cluster (discriminative IDF).
  6. Outputs: clustering_labels.csv (id, department, cluster_id, year); cluster_keywords.csv;
     publisher_topic_distribution.csv, publisher_cluster_counts.csv; interdisciplinary_clusters.csv;
     figures: UMAP/t-SNE/PCA (by cluster and by faculty), publisher×topic heatmap, cluster sizes.

Config: analysis/thesis_semantic_clustering_config.yaml. Run: uv run python analysis/thesis_semantic_clustering.py
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RANDOM_STATE = 42
MIN_ABSTRACT_LEN = 50
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # use multi-qa-mpnet-base-dot-v1 via --embedding-model for better quality (larger download)
CACHE_EMBEDDINGS = "analysis/cache/abstract_embeddings.npy"
CACHE_EMBEDDINGS_FULLTEXT = "analysis/cache/fulltext_embeddings.npy"
CACHE_FULLTEXT_DATA = "analysis/cache/fulltext_data.pkl"
DATA_PATH = "Thesis_meta/thesis_meta_combined_filtered.csv"
DEPARTMENT_CSV_GCS = "dtu_findit/master_thesis_meta/thesis_meta_combined_department.csv"
DEPARTMENT_CSV_LOCAL = Path("Thesis_meta/thesis_meta_combined_department.csv")
RESULTS_DIR = Path("analysis/results")
FIGURES_DIR = Path("analysis/figures")
CACHE_DIR = Path("analysis/cache")
FULLTEXT_MARKDOWN_PREFIX = "dtu_findit/master_thesis_markdown"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "thesis_semantic_clustering_config.yaml"


def load_config(path: str | Path | None = None) -> dict:
    """Load YAML config; merge with defaults so missing keys are filled."""
    defaults = {
        "data_path": DATA_PATH,
        "sample_size": None,
        "embedding_model": EMBEDDING_MODEL,
        "recompute_embeddings": False,
        "cluster_method": "hdbscan",
        "n_clusters": 25,
        "use_fulltext_gcs": False,
        "fulltext_bucket": "thesis_archive_bucket",
        "fulltext_prefix": FULLTEXT_MARKDOWN_PREFIX,
        "use_department_gcs": True,
        "department_csv_gcs": DEPARTMENT_CSV_GCS,
        "department_csv_local": str(DEPARTMENT_CSV_LOCAL),
        "tsne_perplexities": [30],
    }
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        log.warning("Config not found %s; using defaults", cfg_path)
        return defaults
    with open(cfg_path, encoding="utf-8") as f:
        user = yaml.safe_load(f) or {}
    out = {**defaults, **{k: v for k, v in user.items() if v is not None}}
    for k, v in user.items():
        if v is None and k in defaults:
            out[k] = None
    return out


# Column name variants (first match wins)
ABSTRACT_COL_CANDIDATES = ["abstract_ts"]
PUBLISHER_COL_CANDIDATES = ["Publisher"]
ID_COL_CANDIDATES = ["member_id_ss"]


def detect_schema(df: pd.DataFrame) -> dict[str, str]:
    """Detect abstract, publisher, and id column names. Returns dict with keys abstract_col, publisher_col, id_col."""
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
        for c in df.columns:
            if "publish" in c.lower() or "section" in c.lower():
                schema["publisher_col"] = c
                break
        else:
            schema["publisher_col"] = None
    if not schema.get("id_col"):
        schema["id_col"] = df.columns[0]
    return schema


def load_data(path: str | Path = DATA_PATH, sample_size: int | None = None, min_abstract_len: int = MIN_ABSTRACT_LEN) -> pd.DataFrame:
    """Load CSV, detect schema, clean (drop missing/short abstracts), optionally sample."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    df = pd.read_csv(path, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
    log.info("Loaded %d rows from %s", len(df), path)

    schema = detect_schema(df)
    log.info("Schema: abstract=%s, publisher=%s, id=%s", schema.get("abstract_col"), schema.get("publisher_col"), schema.get("id_col"))

    abstract_col = schema["abstract_col"]
    publisher_col = schema.get("publisher_col")
    id_col = schema.get("id_col")

    df[abstract_col] = df[abstract_col].astype(str).str.strip().replace("nan", "")
    if min_abstract_len > 0:
        df = df.dropna(subset=[abstract_col])
    df = df[df[abstract_col].str.len() >= min_abstract_len]
    log.info("After dropping missing/short abstracts: %d rows", len(df))

    if publisher_col and publisher_col not in df.columns:
        publisher_col = None
    if not publisher_col:
        log.warning("No publisher/section column found; publisher analysis will be skipped")

    if sample_size and len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=RANDOM_STATE).reset_index(drop=True)
        log.info("Sampled to %d rows", len(df))

    return df


def _get_gcs_bucket(bucket_name: str):
    try:
        from google.cloud import storage
        return storage.Client().bucket(bucket_name)
    except ImportError:
        raise ImportError("Full-text from GCS requires google-cloud-storage") from None


def load_department_csv_from_gcs(bucket_name: str, blob_path: str, bucket=None) -> pd.DataFrame:
    """Download a single CSV from GCS and return as DataFrame. Expects sep=; and Department_new column."""
    if bucket is None:
        bucket = _get_gcs_bucket(bucket_name)
    raw = bucket.blob(blob_path).download_as_bytes()
    df = pd.read_csv(io.BytesIO(raw), sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
    if "Department_new" not in df.columns:
        raise ValueError(f"Department CSV must have 'Department_new' column; found {list(df.columns)}")
    return df


def load_department_csv_local(path: Path) -> pd.DataFrame:
    """Load department CSV from local file. Expects sep=; and Department_new column."""
    df = pd.read_csv(path, sep=";", encoding="utf-8", on_bad_lines="skip", low_memory=False)
    if "Department_new" not in df.columns:
        raise ValueError(f"Department CSV must have 'Department_new' column; found {list(df.columns)}")
    return df


def merge_department_into_df(df: pd.DataFrame, id_col: str, publisher_col: str | None, department_df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Left-join Department_new from department_df into df; return (df with Department_new), publisher_col to use.
    Rows without a match keep the original publisher (fallback)."""
    id_dept = None
    for c in [id_col, "ID", "id", "member_id_ss"]:
        if c in department_df.columns:
            id_dept = c
            break
    if id_dept is None:
        raise ValueError(f"Department CSV has no ID column (tried {id_col}, ID, id, member_id_ss); columns: {list(department_df.columns)}")
    merged = df.merge(
        department_df[[id_dept, "Department_new"]].drop_duplicates(subset=[id_dept]),
        left_on=id_col,
        right_on=id_dept,
        how="left",
        suffixes=("", "_dept"),
    )
    dept_new = merged["Department_new"]
    if publisher_col and publisher_col in df.columns:
        dept_new = dept_new.fillna(merged[publisher_col])
    dept_new = dept_new.fillna("(missing)")
    df = df.copy()
    df["Department_new"] = dept_new.values
    return df, "Department_new"


def _list_markdown_blobs(bucket, prefix: str) -> dict[str, str]:
    """List blobs under prefix (e.g. dtu_findit/master_thesis_markdown/); return stem -> blob_name.
    Stem = filename without .md; row ids match stem or stem.startswith(row_id + '_')."""
    prefix = prefix.rstrip("/") + "/"
    out = {}
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith(".md"):
            continue
        stem = Path(blob.name).stem
        if stem not in out:
            out[stem] = blob.name
        # Prefer exact id match: if we later have abc123 and abc123_title, keep first (arbitrary)
    return out


def _blob_for_row_id(stem_to_blob: dict[str, str], row_id: str) -> str | None:
    row_id = row_id.strip()
    if row_id in stem_to_blob:
        return stem_to_blob[row_id]
    for stem, blob_name in stem_to_blob.items():
        if stem.startswith(row_id + "_"):
            return blob_name
    return None


def load_fulltext_from_gcs(
    df: pd.DataFrame,
    id_col: str,
    bucket_name: str,
    prefix: str,
    bucket=None,
) -> tuple[pd.DataFrame, list[str]]:
    """Filter df to rows that have a markdown blob; return (filtered df, list of full-text strings)."""
    if bucket is None:
        bucket = _get_gcs_bucket(bucket_name)
    stem_to_blob = _list_markdown_blobs(bucket, prefix)
    log.info("Found %d markdown blobs under gs://%s/%s", len(stem_to_blob), bucket.name, prefix)

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = lambda x, **kw: x
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
            if len(content.strip()) < MIN_ABSTRACT_LEN:
                continue
            indices.append(i)
            texts.append(content.strip())
            break
        else:
            continue

    filtered = df.loc[indices].reset_index(drop=True)
    log.info("Loaded full text for %d theses (from %d meta rows)", len(texts), len(df))
    return filtered, texts


def chunk_text_for_embedding(text: str, chunk_chars: int = 1600, overlap_chars: int = 200) -> list[str]:
    """Split text into overlapping character chunks (~400 tokens at ~4 chars/token)."""
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_chars, len(text))
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = end - overlap_chars
    return chunks


def get_embedding_model(model_name: str | None = None, local_files_only: bool = True):
    try:
        from sentence_transformers import SentenceTransformer
        name = model_name or EMBEDDING_MODEL
        return SentenceTransformer(name, local_files_only=local_files_only)
    except ImportError:
        raise ImportError("Install sentence-transformers: pip install sentence-transformers") from None


def compute_embeddings(
    abstracts: list[str],
    cache_path: Path | None = None,
    recompute: bool = False,
    embedding_model: str | None = None,
) -> np.ndarray:
    """Embed abstracts; load from cache if present and not recompute. Returns L2-normalized embeddings."""
    from sklearn.preprocessing import normalize
    cache_path = cache_path or Path(CACHE_EMBEDDINGS)
    if cache_path.exists() and not recompute:
        emb = np.load(cache_path)
        if len(emb) == len(abstracts):
            log.info("Loaded embeddings from cache %s, shape %s", cache_path, emb.shape)
            return normalize(emb, norm="l2")
    model = get_embedding_model(embedding_model)
    emb = model.encode(abstracts, show_progress_bar=True)
    emb = normalize(emb, norm="l2")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(cache_path, emb)
    log.info("Computed and cached embeddings shape %s to %s", emb.shape, cache_path)
    return emb


def compute_embeddings_chunked(
    fulltexts: list[str],
    cache_path: Path | None = None,
    recompute: bool = False,
    chunk_chars: int = 1600,
    overlap_chars: int = 200,
    titles: list[str] | None = None,
    embedding_model: str | None = None,
) -> np.ndarray:
    """Chunk each doc, embed chunks, weighted average (intro/conclusion up-weighted) to one vector per doc. Optional title prepended to chunks. Returns L2-normalized."""
    from sklearn.preprocessing import normalize
    cache_path = cache_path or Path(CACHE_EMBEDDINGS_FULLTEXT)
    if cache_path.exists() and not recompute:
        emb = np.load(cache_path)
        if len(emb) == len(fulltexts):
            log.info("Loaded fulltext embeddings from cache %s, shape %s", cache_path, emb.shape)
            return normalize(emb, norm="l2")
    model = get_embedding_model(embedding_model)
    if titles is None:
        titles = [""] * len(fulltexts)
    all_embeddings = []
    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = lambda x, **kw: x
    for i, text in tqdm(enumerate(fulltexts), total=len(fulltexts), desc="Chunk-embed theses"):
        chunks = chunk_text_for_embedding(text, chunk_chars=chunk_chars, overlap_chars=overlap_chars)
        title = (titles[i] or "").strip()
        if title:
            chunks = [f"{title}. {c}" for c in chunks]
        if not chunks:
            chunk_emb = model.encode([text[:5000] or " "], show_progress_bar=False)
            all_embeddings.append(chunk_emb[0])
            continue
        chunk_emb = model.encode(chunks, show_progress_bar=False)
        weights = np.ones(len(chunks))
        weights[0] = 2.0
        weights[-1] = 1.5
        all_embeddings.append(np.average(chunk_emb, axis=0, weights=weights))
    emb = np.stack(all_embeddings)
    emb = normalize(emb, norm="l2")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(cache_path, emb)
    log.info("Computed and cached fulltext embeddings shape %s to %s", emb.shape, cache_path)
    return emb


def reduce_umap(embeddings: np.ndarray, n_components: int = 2, random_state: int = RANDOM_STATE) -> np.ndarray | None:
    try:
        import umap
    except ImportError:
        log.warning("umap-learn not installed; skipping UMAP")
        return None
    reducer = umap.UMAP(n_components=n_components, random_state=random_state)
    return reducer.fit_transform(embeddings)


def reduce_tsne(
    embeddings: np.ndarray,
    perplexity: float,
    n_components: int = 2,
    random_state: int = RANDOM_STATE,
) -> np.ndarray | None:
    """t-SNE 2D for a given perplexity (can be slow on large data)."""
    try:
        from sklearn.manifold import TSNE
    except ImportError:
        return None
    n = len(embeddings)
    if n <= 1 or perplexity >= n:
        return None
    tsne = TSNE(n_components=n_components, random_state=random_state, perplexity=perplexity)
    return tsne.fit_transform(embeddings)


def reduce_pca(embeddings: np.ndarray, n_components: int = 2, random_state: int = RANDOM_STATE) -> np.ndarray:
    """PCA 2D for a fast linear 2D view."""
    from sklearn.decomposition import PCA
    pca = PCA(n_components=n_components, random_state=random_state)
    return pca.fit_transform(embeddings)


def cluster_hdbscan(embeddings: np.ndarray, min_cluster_size: int = 15, min_samples: int = 5) -> np.ndarray:
    try:
        import hdbscan
    except ImportError:
        raise ImportError("Install hdbscan: pip install hdbscan") from None
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
        cluster_selection_method="leaf",
    )
    return clusterer.fit_predict(embeddings)


def cluster_kmeans(embeddings: np.ndarray, n_clusters: int = 20, random_state: int = RANDOM_STATE) -> np.ndarray:
    from sklearn.cluster import KMeans
    km = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    return km.fit_predict(embeddings)


def cluster_agglomerative(embeddings: np.ndarray, n_clusters: int = 20) -> np.ndarray:
    from sklearn.cluster import AgglomerativeClustering
    ac = AgglomerativeClustering(n_clusters=n_clusters)
    return ac.fit_predict(embeddings)


def extract_cluster_keywords(abstracts: list[str], labels: np.ndarray, top_n: int = 15) -> pd.DataFrame:
    """TF-IDF fit on all docs once, transform per cluster; top keywords per cluster."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    vectorizer = TfidfVectorizer(max_features=5000, stop_words="english", min_df=2)
    vectorizer.fit(abstracts)
    rows = []
    for c in sorted(set(labels)):
        mask = labels == c
        docs = [abstracts[i] for i in range(len(abstracts)) if mask[i]]
        if not docs:
            continue
        tfidf = vectorizer.transform(docs)
        sums = np.asarray(tfidf.sum(axis=0)).flatten()
        idx = np.argsort(-sums)[:top_n]
        words = [vectorizer.get_feature_names_out()[i] for i in idx]
        rows.append({"cluster_id": int(c), "cluster_size": int(mask.sum()), "top_keywords": "|".join(words)})
    return pd.DataFrame(rows)


def publisher_topic_distribution(df: pd.DataFrame, publisher_col: str, labels: np.ndarray) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Counts and normalized distribution: publisher x cluster."""
    df = df.copy()
    df["_cluster"] = labels
    counts = df.groupby([publisher_col, "_cluster"]).size().unstack(fill_value=0)
    totals = counts.sum(axis=1)
    norm = counts.div(totals, axis=0)
    return counts, norm


def interdisciplinary_score(df: pd.DataFrame, publisher_col: str, labels: np.ndarray) -> pd.DataFrame:
    """Per cluster: number of publishers and entropy of publisher distribution."""
    from scipy.stats import entropy
    rows = []
    for c in sorted(set(labels)):
        mask = labels == c
        sub = df.loc[mask, publisher_col]
        counts = sub.value_counts()
        n_pubs = len(counts)
        probs = counts / counts.sum()
        ent = entropy(probs)
        rows.append({"cluster_id": int(c), "cluster_size": int(mask.sum()), "number_of_publishers": n_pubs, "publisher_entropy": ent})
    out = pd.DataFrame(rows).sort_values("publisher_entropy", ascending=False)
    return out


def run_pipeline(
    data_path: str | Path = DATA_PATH,
    sample_size: int | None = None,
    recompute_embeddings: bool = False,
    cluster_method: str = "hdbscan",
    n_clusters: int = 25,
    use_fulltext_gcs: bool = False,
    fulltext_bucket: str | None = None,
    fulltext_prefix: str | None = None,
    use_department_gcs: bool = True,
    department_csv_gcs: str | None = None,
    department_csv_local: str | Path | None = None,
    embedding_model: str | None = None,
    tsne_perplexities: list[float] | None = None,
) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    model_name = embedding_model or EMBEDDING_MODEL
    _model_slug = model_name.replace("/", "_")
    cache_path_abstract = CACHE_DIR / f"abstract_embeddings_{_model_slug}.npy"
    cache_path_fulltext = CACHE_DIR / f"fulltext_embeddings_{_model_slug}.npy"

    # 1. Load meta (when using fulltext, do not require abstract so we keep rows that only have markdown)
    min_len = 0 if use_fulltext_gcs else MIN_ABSTRACT_LEN
    df = load_data(data_path, sample_size=sample_size, min_abstract_len=min_len)
    schema = detect_schema(df)
    abstract_col = schema["abstract_col"]
    publisher_col = schema.get("publisher_col")
    if "Department_new" in df.columns:
        publisher_col = "Department_new"
    id_col = schema.get("id_col")

    if use_fulltext_gcs:
        bucket_name = fulltext_bucket or os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
        prefix = fulltext_prefix or FULLTEXT_MARKDOWN_PREFIX
        fulltext_data_path = Path(CACHE_FULLTEXT_DATA)
        cache_key = (str(Path(data_path).resolve()), bucket_name, prefix, sample_size)
        if fulltext_data_path.exists():
            try:
                with open(fulltext_data_path, "rb") as f:
                    cached = pickle.load(f)
                if (cached.get("data_path") == cache_key[0] and cached.get("bucket") == cache_key[1]
                        and cached.get("prefix") == cache_key[2] and cached.get("sample_size") == cache_key[3]):
                    df = cached["df"]
                    texts = cached["texts"]
                    log.info("Loaded fulltext from cache %s (%d theses)", fulltext_data_path, len(texts))
                else:
                    cached = None
            except Exception as e:
                log.warning("Fulltext cache read failed: %s; re-downloading from GCS", e)
                cached = None
        else:
            cached = None
        if cached is None:
            fulltext_id_col = "member_id_ss" if "member_id_ss" in df.columns else id_col
            df, texts = load_fulltext_from_gcs(df, fulltext_id_col, bucket_name, prefix)
            if len(df) == 0:
                raise SystemExit("No rows with markdown full text found in GCS. Check bucket and prefix.")
            fulltext_data_path.parent.mkdir(parents=True, exist_ok=True)
            with open(fulltext_data_path, "wb") as f:
                pickle.dump({"df": df, "texts": texts, "data_path": cache_key[0], "bucket": bucket_name, "prefix": prefix, "sample_size": sample_size}, f)
            log.info("Cached fulltext to %s", fulltext_data_path)
        doc_label = "full text"
        cache_path = cache_path_fulltext
        title_col = next((c for c in df.columns if str(c).strip().lower() == "title"), None)
        titles = df[title_col].astype(str).tolist() if title_col else None
        embeddings = compute_embeddings_chunked(
            texts, cache_path=cache_path, recompute=recompute_embeddings, titles=titles, embedding_model=model_name
        )
    else:
        texts = df[abstract_col].tolist()
        doc_label = "abstracts"
        cache_path = cache_path_abstract
        embeddings = compute_embeddings(
            texts, cache_path=cache_path, recompute=recompute_embeddings, embedding_model=model_name
        )

    _dept_csv = Path(department_csv_local) if department_csv_local else DEPARTMENT_CSV_LOCAL
    if "Department_new" not in df.columns and _dept_csv.exists():
        log.info("Loading Department_new from local %s", _dept_csv)
        department_df = load_department_csv_local(_dept_csv)
        df, publisher_col = merge_department_into_df(df, id_col, publisher_col, department_df)
        log.info("Using Department_new as faculty column (%d unique)", df["Department_new"].nunique())
    if use_department_gcs:
        bucket_name = fulltext_bucket or os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
        blob_path = department_csv_gcs or DEPARTMENT_CSV_GCS
        log.info("Loading Department_new from gs://%s/%s", bucket_name, blob_path)
        department_df = load_department_csv_from_gcs(bucket_name, blob_path)
        df, publisher_col = merge_department_into_df(df, id_col, publisher_col, department_df)
        log.info("Using Department_new as faculty column (%d unique)", df["Department_new"].nunique())

    log.info("Dataset size: %d", len(df))
    log.info("Embedding dimension: %d", embeddings.shape[1])
    # Embeddings are already saved to cache_path; we drop them after each use to reduce memory.

    # 3. UMAP + clustering (then drop embeddings from memory)
    umap_2d = reduce_umap(embeddings, n_components=2) if embeddings is not None else None
    umap_10 = reduce_umap(embeddings, n_components=10) if embeddings is not None else None
    emb_for_cluster = umap_10 if umap_10 is not None else embeddings
    if embeddings is not None:
        del embeddings
    # 4. Cluster
    if cluster_method == "hdbscan":
        labels = cluster_hdbscan(emb_for_cluster)
    elif cluster_method == "kmeans":
        labels = cluster_kmeans(emb_for_cluster, n_clusters=n_clusters)
    elif cluster_method == "agg":
        labels = cluster_agglomerative(emb_for_cluster, n_clusters=n_clusters)
    else:
        labels = cluster_hdbscan(emb_for_cluster)
    del emb_for_cluster
    if umap_10 is not None:
        del umap_10
    n_clusters_found = len(set(labels)) - (1 if -1 in labels else 0)
    log.info("Clustering (%s): %d clusters (noise points: %d)", cluster_method, n_clusters_found, (labels == -1).sum())

    # t-SNE and PCA: load embeddings from file only when needed, then drop
    from sklearn.preprocessing import normalize as _normalize
    perplexities = tsne_perplexities if tsne_perplexities is not None else [30]
    tsne_results: dict[float, np.ndarray] = {}
    if perplexities:
        log.info("Computing t-SNE 2D for perplexities %s (can be slow for large n)...", perplexities)
        embeddings = np.load(cache_path) if cache_path.exists() else None
        if embeddings is not None:
            embeddings = _normalize(embeddings, norm="l2")
            for p in perplexities:
                arr = reduce_tsne(embeddings, perplexity=p)
                if arr is not None:
                    tsne_results[p] = arr
            del embeddings
    embeddings = np.load(cache_path) if cache_path.exists() else None
    if embeddings is not None:
        embeddings = _normalize(embeddings, norm="l2")
    pca_2d = reduce_pca(embeddings, n_components=2) if embeddings is not None else None
    del embeddings

    # 4b. Save per-thesis labels (and optional year) for faculty-level analysis
    out_df = df[[id_col, publisher_col]].copy()
    out_df["cluster_id"] = labels
    date_col = None
    for c in ["Year", "year", "Publication Year", "date", "Date", "graduation_year"]:
        if c in df.columns:
            date_col = c
            break
    if date_col:
        out_df["year"] = df[date_col]
    out_df.to_csv(RESULTS_DIR / "clustering_labels.csv", index=False)
    log.info("Saved %s", RESULTS_DIR / "clustering_labels.csv")

    # 5. Cluster keywords (use same texts we embedded)
    kw_df = extract_cluster_keywords(texts, labels)
    kw_path = RESULTS_DIR / "cluster_keywords.csv"
    kw_df.to_csv(kw_path, index=False)
    log.info("Saved %s", kw_path)

    # 6. Publisher topic distribution
    if publisher_col:
        counts_df, norm_df = publisher_topic_distribution(df, publisher_col, labels)
        dist_path = RESULTS_DIR / "publisher_topic_distribution.csv"
        norm_df.to_csv(dist_path)
        log.info("Saved %s", dist_path)
        counts_path = RESULTS_DIR / "publisher_cluster_counts.csv"
        counts_df.to_csv(counts_path)
        inter_df = interdisciplinary_score(df, publisher_col, labels)
    else:
        norm_df = None
        counts_df = None
        inter_df = None

    # 7. Figures
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap

    def _scatter_by_faculty(ax, xy: np.ndarray, publisher_series, doc_label: str, xlabel: str, ylabel: str):
        """Scatter xy colored by faculty (Publisher); add legend."""
        pubs = publisher_series.fillna("(missing)").astype(str)
        uniq = sorted(pubs.unique())
        pub_to_idx = {p: i for i, p in enumerate(uniq)}
        idx = np.array([pub_to_idx[p] for p in pubs])
        n = len(uniq)
        base = plt.cm.tab20(np.linspace(0, 1, min(20, n)))
        if n > 20:
            base = np.tile(plt.cm.tab20(np.linspace(0, 1, 20)), (n // 20 + 1, 1))[:n]
        cmap = ListedColormap(base)
        scatter = ax.scatter(xy[:, 0], xy[:, 1], c=idx, cmap=cmap, s=5, alpha=0.6)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        handles = [plt.scatter([], [], c=[cmap(i)], s=30, alpha=0.8, label=p) for i, p in enumerate(uniq)]
        ax.legend(handles=handles, loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=7)
        return scatter

    if umap_2d is not None:
        fig, ax = plt.subplots(figsize=(10, 8))
        scatter = ax.scatter(umap_2d[:, 0], umap_2d[:, 1], c=labels, cmap="tab20", s=5, alpha=0.6)
        plt.colorbar(scatter, ax=ax, label="Cluster")
        ax.set_title(f"UMAP of thesis {doc_label} (colored by cluster)")
        ax.set_xlabel("UMAP 1")
        ax.set_ylabel("UMAP 2")
        fig.savefig(FIGURES_DIR / "embedding_umap_clusters.png", dpi=150, bbox_inches="tight")
        plt.close()
        log.info("Saved %s", FIGURES_DIR / "embedding_umap_clusters.png")

        if publisher_col:
            fig, ax = plt.subplots(figsize=(12, 8))
            _scatter_by_faculty(ax, umap_2d, df[publisher_col], doc_label, "UMAP 1", "UMAP 2")
            ax.set_title(f"UMAP of thesis {doc_label} (colored by faculty)")
            fig.tight_layout(rect=[0, 0, 0.85, 1])
            fig.savefig(FIGURES_DIR / "embedding_umap_faculty.png", dpi=150, bbox_inches="tight")
            plt.close()
            log.info("Saved %s", FIGURES_DIR / "embedding_umap_faculty.png")

        # UMAP colored by interdisciplinary (publisher entropy per cluster)
        if inter_df is not None and publisher_col:
            cluster_to_entropy = inter_df.set_index("cluster_id")["publisher_entropy"].to_dict()
            entropy_per_point = np.array([cluster_to_entropy.get(int(l), 0.0) for l in labels])
            fig, ax = plt.subplots(figsize=(10, 8))
            sc = ax.scatter(umap_2d[:, 0], umap_2d[:, 1], c=entropy_per_point, cmap="viridis", s=5, alpha=0.6)
            plt.colorbar(sc, ax=ax, label="Publisher entropy (interdisciplinary)")
            ax.set_title("UMAP colored by interdisciplinary (high = many sections)")
            ax.set_xlabel("UMAP 1")
            ax.set_ylabel("UMAP 2")
            fig.savefig(FIGURES_DIR / "embedding_umap_interdisciplinary.png", dpi=150, bbox_inches="tight")
            plt.close()
            log.info("Saved %s", FIGURES_DIR / "embedding_umap_interdisciplinary.png")

    if tsne_results:
        for p, tsne_2d in tsne_results.items():
            fig, ax = plt.subplots(figsize=(10, 8))
            scatter = ax.scatter(tsne_2d[:, 0], tsne_2d[:, 1], c=labels, cmap="tab20", s=5, alpha=0.6)
            plt.colorbar(scatter, ax=ax, label="Cluster")
            ax.set_title(f"t-SNE (perplexity={p}) of thesis {doc_label} (colored by cluster)")
            ax.set_xlabel("t-SNE 1")
            ax.set_ylabel("t-SNE 2")
            fig.savefig(FIGURES_DIR / f"embedding_tsne_clusters_p{int(p)}.png", dpi=150, bbox_inches="tight")
            plt.close()
            log.info("Saved %s", FIGURES_DIR / f"embedding_tsne_clusters_p{int(p)}.png")
            if publisher_col:
                fig, ax = plt.subplots(figsize=(12, 8))
                _scatter_by_faculty(ax, tsne_2d, df[publisher_col], doc_label, "t-SNE 1", "t-SNE 2")
                ax.set_title(f"t-SNE (perplexity={p}) of thesis {doc_label} (colored by faculty)")
                fig.tight_layout(rect=[0, 0, 0.85, 1])
                fig.savefig(FIGURES_DIR / f"embedding_tsne_faculty_p{int(p)}.png", dpi=150, bbox_inches="tight")
                plt.close()
                log.info("Saved %s", FIGURES_DIR / f"embedding_tsne_faculty_p{int(p)}.png")

    if pca_2d is not None:
        fig, ax = plt.subplots(figsize=(10, 8))
        scatter = ax.scatter(pca_2d[:, 0], pca_2d[:, 1], c=labels, cmap="tab20", s=5, alpha=0.6)
        plt.colorbar(scatter, ax=ax, label="Cluster")
        ax.set_title(f"PCA of thesis {doc_label} (colored by cluster)")
        ax.set_xlabel("PC1")
        ax.set_ylabel("PC2")
        fig.savefig(FIGURES_DIR / "embedding_pca_clusters.png", dpi=150, bbox_inches="tight")
        plt.close()
        log.info("Saved %s", FIGURES_DIR / "embedding_pca_clusters.png")
        if publisher_col:
            fig, ax = plt.subplots(figsize=(12, 8))
            _scatter_by_faculty(ax, pca_2d, df[publisher_col], doc_label, "PC1", "PC2")
            ax.set_title(f"PCA of thesis {doc_label} (colored by faculty)")
            fig.tight_layout(rect=[0, 0, 0.85, 1])
            fig.savefig(FIGURES_DIR / "embedding_pca_faculty.png", dpi=150, bbox_inches="tight")
            plt.close()
            log.info("Saved %s", FIGURES_DIR / "embedding_pca_faculty.png")

    if publisher_col and norm_df is not None:
        import matplotlib.pyplot as plt
        import seaborn as sns
        fig, ax = plt.subplots(figsize=(14, max(6, norm_df.shape[0] * 0.25)))
        sns.heatmap(norm_df, ax=ax, cmap="YlOrRd")
        ax.set_title("Publisher × Topic (normalized)")
        fig.savefig(FIGURES_DIR / "publisher_topic_heatmap.png", dpi=150, bbox_inches="tight")
        plt.close()
        log.info("Saved %s", FIGURES_DIR / "publisher_topic_heatmap.png")

    size_counts = pd.Series(labels).value_counts()
    if -1 in size_counts.index:
        size_counts = size_counts.drop(-1)
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 5))
    size_counts.head(30).plot(kind="bar", ax=ax)
    ax.set_title("Largest clusters (by size)")
    ax.set_ylabel("Count")
    fig.savefig(FIGURES_DIR / "cluster_size_distribution.png", dpi=150, bbox_inches="tight")
    plt.close()
    log.info("Saved %s", FIGURES_DIR / "cluster_size_distribution.png")

    # 8. Interdisciplinary (inter_df already computed in step 6 for the UMAP plot)
    if inter_df is not None:
        inter_path = RESULTS_DIR / "interdisciplinary_clusters.csv"
        inter_df.to_csv(inter_path, index=False)
        log.info("Saved %s", inter_path)


def main():
    parser = argparse.ArgumentParser(description="Thesis semantic clustering (config-driven)")
    parser.add_argument("--config", default=None, help="Path to YAML config (default: analysis/thesis_semantic_clustering_config.yaml)")
    parser.add_argument("--schema-only", action="store_true", help="Print schema summary and exit")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if args.schema_only:
        df = load_data(cfg["data_path"])
        schema = detect_schema(df)
        print("Schema summary:")
        print("  abstract_col:", schema.get("abstract_col"))
        print("  publisher_col:", schema.get("publisher_col"))
        print("  id_col:", schema.get("id_col"))
        print("  rows:", len(df))
        print("  columns:", list(df.columns))
        return

    run_pipeline(
        data_path=cfg["data_path"],
        sample_size=cfg.get("sample_size"),
        recompute_embeddings=cfg.get("recompute_embeddings", False),
        cluster_method=cfg.get("cluster_method", "hdbscan"),
        n_clusters=cfg.get("n_clusters", 25),
        use_fulltext_gcs=cfg.get("use_fulltext_gcs", False),
        fulltext_bucket=cfg.get("fulltext_bucket"),
        fulltext_prefix=cfg.get("fulltext_prefix"),
        use_department_gcs=cfg.get("use_department_gcs", True),
        department_csv_gcs=cfg.get("department_csv_gcs"),
        department_csv_local=cfg.get("department_csv_local"),
        embedding_model=cfg.get("embedding_model"),
        tsne_perplexities=cfg.get("tsne_perplexities"),
    )


if __name__ == "__main__":
    main()
