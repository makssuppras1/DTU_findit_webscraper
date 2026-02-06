"""GCS upload and progress tracking."""

import json
import logging
import os
import re
from pathlib import Path

from google.cloud import storage

from config import GCS_PREFIX, PROGRESS_FILE

log = logging.getLogger(__name__)


def get_project():
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        return os.environ["GOOGLE_CLOUD_PROJECT"]
    import subprocess
    try:
        out = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return None


def get_gcs_client():
    project = get_project()
    try:
        return storage.Client(project=project) if project else storage.Client()
    except Exception as e:
        raise SystemExit(
            f"GCS auth failed: {e}\n"
            "Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path, or run:\n"
            "  gcloud auth application-default login"
        ) from e


def get_bucket():
    name = os.environ.get("GCS_BUCKET")
    if not name:
        raise SystemExit("Set GCS_BUCKET environment variable.")
    return get_gcs_client().bucket(name)


def sanitize_filename(title: str, max_len: int = 120) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "", title)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len] if s else "untitled"


def blob_key(record_id: str, title: str = "") -> str:
    name = sanitize_filename(title) if title else record_id
    return f"{GCS_PREFIX}/{record_id}_{name}.pdf"


def blob_exists(bucket, record_id: str) -> bool:
    prefix = f"{GCS_PREFIX}/{record_id}_"
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
    return len(blobs) > 0


def load_progress() -> set[str]:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
            return set(data.get("processed_record_ids", []))
    return set()


def save_progress(processed: set[str]):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"processed_record_ids": list(processed)}, f, indent=2)


def save_progress_add(record_id: str):
    try:
        import fcntl
    except ImportError:
        fcntl = None

    def _do_save(f):
        data = json.load(f)
        ids = set(data.get("processed_record_ids", []))
        ids.add(record_id)
        data["processed_record_ids"] = list(ids)
        f.seek(0)
        json.dump(data, f, indent=2)
        f.truncate()

    try:
        with open(PROGRESS_FILE, "r+") as f:
            if fcntl:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                except OSError:
                    pass
            try:
                _do_save(f)
            finally:
                if fcntl:
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass
    except FileNotFoundError:
        save_progress({record_id})


def _gcs_metadata(meta: dict) -> dict:
    out = {}
    for k, v in meta.items():
        key = re.sub(r"[^a-zA-Z0-9_-]", "_", str(k))[:256].lower()
        if key:
            out[key] = str(v)[:1024]
    return out


def upload_to_gcs(bucket, local_path: Path, record_id: str, title: str = "", metadata: dict | None = None):
    key = blob_key(record_id, title)
    blob = bucket.blob(key)
    if metadata:
        blob.metadata = _gcs_metadata(metadata)
    blob.upload_from_filename(str(local_path), content_type="application/pdf")


def sync_progress_with_bucket():
    bucket = get_bucket()
    prefix = f"{GCS_PREFIX}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    in_bucket = set()
    for b in blobs:
        name = b.name[len(prefix):]
        if "_" in name:
            record_id = name.split("_", 1)[0]
            if re.match(r"^[a-f0-9\-]{20,}$", record_id, re.I):
                in_bucket.add(record_id)
    processed = load_progress()
    removed = processed - in_bucket
    new_processed = processed & in_bucket
    save_progress(new_processed)
    log.info("Sync: %d blobs in bucket, %d in progress. Removed %d stale IDs.", len(in_bucket), len(processed), len(removed))
