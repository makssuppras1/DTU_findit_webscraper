"""GCS I/O: list PDFs, download bytes, exists, upload JSON."""

import json
from collections.abc import Iterable
from dataclasses import dataclass

from google.cloud import storage


@dataclass
class BlobMeta:
    bucket: str
    name: str
    size: int | None = None


def _get_client():
    try:
        return storage.Client()
    except Exception as e:
        raise SystemExit(
            f"GCS auth failed: {e}. Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
        ) from e


def list_pdfs(bucket_name: str, prefix: str) -> Iterable[BlobMeta]:
    client = _get_client()
    bucket = client.bucket(bucket_name)
    norm_prefix = prefix.rstrip("/") + "/" if prefix else ""
    blobs = list(bucket.list_blobs(prefix=norm_prefix))
    pdf_blobs = [b for b in blobs if b.name.lower().endswith(".pdf")]
    pdf_blobs.sort(key=lambda b: b.name)
    for b in pdf_blobs:
        yield BlobMeta(bucket=bucket_name, name=b.name, size=b.size)


def open_pdf_bytes(bucket_name: str, blob_name: str) -> bytes:
    return _get_client().bucket(bucket_name).blob(blob_name).download_as_bytes()


def gcs_exists(bucket_name: str, blob_name: str) -> bool:
    return _get_client().bucket(bucket_name).blob(blob_name).exists()


def upload_json(bucket_name: str, blob_name: str, data_dict: dict) -> None:
    _get_client().bucket(bucket_name).blob(blob_name).upload_from_string(
        json.dumps(data_dict, ensure_ascii=False), content_type="application/json"
    )
