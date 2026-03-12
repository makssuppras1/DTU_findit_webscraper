"""
Download a SentenceTransformer model into the Hugging Face cache.
Run once when on a good connection; then thesis_semantic_clustering.py will load from cache.

  uv run python analysis/download_embedding_model.py
  uv run python analysis/download_embedding_model.py --model sentence-transformers/all-mpnet-base-v2
"""
from __future__ import annotations

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Download embedding model to HF cache")
    parser.add_argument(
        "--model",
        default="sentence-transformers/multi-qa-mpnet-base-dot-v1",
        help="Model ID (default: multi-qa-mpnet-base-dot-v1)",
    )
    args = parser.parse_args()

    from huggingface_hub import snapshot_download

    cache_dir = snapshot_download(args.model)
    print(f"Downloaded to: {cache_dir}")
    print("Run thesis_semantic_clustering.py; it will load from cache.")


if __name__ == "__main__":
    main()
