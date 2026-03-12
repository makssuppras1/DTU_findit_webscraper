"""Discover source profiles for a researcher (ORCID, orbit_id). MVP: no profile URL scraping."""

from datetime import UTC, datetime

from publication_retrieval.models import ResearcherCandidate, SourceProfile


def discover_profiles(candidate: ResearcherCandidate) -> list[SourceProfile]:
    """Build list of SourceProfile from candidate. Tier 1: orcid, dtu_orbit. No profile_url fetch in MVP."""
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    profiles: list[SourceProfile] = []

    if candidate.orcid:
        orcid_clean = candidate.orcid.strip()
        if not orcid_clean.startswith("https://"):
            orcid_clean = orcid_clean.replace("http://orcid.org/", "").replace("orcid.org/", "").strip()
        profiles.append(
            SourceProfile(
                source="orcid",
                source_id=orcid_clean,
                tier=1,
                discovered_at_iso=now,
                url=f"https://orcid.org/{orcid_clean}" if orcid_clean else None,
            )
        )
        orcid_url = f"https://orcid.org/{orcid_clean}" if orcid_clean else orcid_clean
        profiles.append(
            SourceProfile(
                source="openalex",
                source_id=orcid_url,
                tier=2,
                discovered_at_iso=now,
                url=f"https://openalex.org/search?q={orcid_url}",
            )
        )

    if candidate.orbit_id:
        profiles.append(
            SourceProfile(
                source="dtu_orbit",
                source_id=candidate.orbit_id.strip(),
                tier=1,
                discovered_at_iso=now,
                url=None,
            )
        )

    return profiles
