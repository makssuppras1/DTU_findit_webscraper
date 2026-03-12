"""Load persons CSV from GCS and normalize into ResearcherCandidate."""

import csv
import os
import re
from io import StringIO

from publication_retrieval.models import ResearcherCandidate


def get_bucket(bucket_name: str | None = None):
    from google.cloud import storage
    name = bucket_name or os.environ.get("GCS_BUCKET", "thesis_archive_bucket")
    client = storage.Client()
    return client.bucket(name)


def load_persons_csv_from_gcs(
    gcs_path: str,
    bucket_name: str | None = None,
) -> list[dict[str, str]]:
    """Download CSV from GCS and return list of row dicts (header -> value)."""
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    raw = blob.download_as_bytes().decode("utf-8", errors="replace")
    reader = csv.DictReader(StringIO(raw))
    return list(reader)


def _get(row: dict, *keys: str, default: str = "") -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


def _normalize_orcid(s: str) -> str | None:
    if not s or not s.strip():
        return None
    s = re.sub(r"[\s\-]", "", s.strip())
    m = re.match(r"^(\d{4})\-?(\d{4})\-?(\d{4})\-?(\d{3}[\dX])$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}"
    if re.match(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", s):
        return s
    return None


def _canonical_name(parts: list[str]) -> str:
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0].strip()
    return f"{parts[-1].strip()}, {' '.join(p.strip() for p in parts[:-1])}"


def normalize_identity(row: dict, row_index: int) -> ResearcherCandidate:
    """Map CSV row to ResearcherCandidate. Tolerates name / first_name+last_name, orbit_id / member_id_ss."""
    name = _get(row, "name", "Name", "full_name")
    if not name:
        first = _get(row, "first_name", "First name", "given_name")
        last = _get(row, "last_name", "Last name", "family_name", "surname")
        name = _canonical_name([first, last]) if (first or last) else last or first
    if "," in name:
        pass
    else:
        parts = name.split()
        name = _canonical_name(parts) if len(parts) > 1 else name

    orcid_raw = _get(row, "orcid", "ORCID", "orcid_id")
    orcid = _normalize_orcid(orcid_raw) if orcid_raw else None

    email = _get(row, "email", "Email") or None
    department = _get(row, "department", "Department") or None
    institution = _get(row, "institution", "Institution", "organization") or None
    if not institution and email and "@" in email:
        domain = email.split("@")[-1].lower()
        if domain == "dtu.dk":
            institution = "DTU"
    profile_url = _get(row, "profile_url", "Profile URL", "url", "profile") or None
    orbit_id = _get(row, "orbit_id", "orbit_id", "member_id_ss", "member_id") or None

    return ResearcherCandidate(
        input_id=f"row_{row_index}",
        canonical_name=name or f"Unknown_{row_index}",
        orcid=orcid,
        email=email or None,
        department=department or None,
        institution=institution or None,
        profile_url=profile_url or None,
        orbit_id=orbit_id or None,
    )


def load_persons_from_gcs(
    gcs_path: str,
    bucket_name: str | None = None,
) -> list[ResearcherCandidate]:
    """Load CSV from GCS and return list of ResearcherCandidate."""
    rows = load_persons_csv_from_gcs(gcs_path, bucket_name)
    return [normalize_identity(row, i) for i, row in enumerate(rows) if any(str(v).strip() for v in row.values())]
