import base64
import re
from bs4 import BeautifulSoup

from .models import PersonRecord


def _text(elem) -> str:
    if elem is None:
        return ""
    return elem.get_text(separator=" ", strip=True)


def _deobfuscate_email(soup) -> str:
    email_link = soup.find("a", class_="email")
    if not email_link:
        return ""
    data_md5 = email_link.get("data-md5")
    if data_md5:
        try:
            decoded = base64.b64decode(data_md5).decode("utf-8", errors="ignore")
            if decoded.startswith("mailto:"):
                return decoded[7:].strip()
            return decoded.strip()
        except Exception:
            pass
    text = email_link.get_text(separator="", strip=True)
    text = re.sub(r"encryptedA\s*\(\s*\)", "@", text, flags=re.I)
    text = re.sub(r"encryptedDot\s*\(\s*\)", ".", text, flags=re.I)
    if "@" in text and "." in text:
        return text
    return ""


def _section_after_heading(soup: BeautifulSoup, heading_text: str) -> str:
    for h in soup.find_all(["h2", "h3", "h4"]):
        if heading_text.lower() in _text(h).lower():
            container = h.find_parent("section") or h.find_parent("div")
            if container:
                return _text(container)
            next_ = h.find_next_sibling()
            if next_:
                return _text(next_)
    return ""


def parse_profile(html: str, url: str) -> PersonRecord:
    soup = BeautifulSoup(html, "lxml")
    name = _text(soup.find("h1")) or ""

    affiliations_parts = []
    aff_block = soup.find(class_=re.compile(r"personorganisationlistrenderer"))
    if aff_block:
        for li in aff_block.find_all("li"):
            job = li.find(class_="job-title")
            dept = li.find("a", class_=re.compile(r"department|link"))
            parts = []
            if job:
                parts.append(_text(job))
            if dept:
                parts.append(_text(dept))
            if parts:
                affiliations_parts.append(", ".join(parts))
    affiliations = "; ".join(affiliations_parts) if affiliations_parts else ""

    email = _deobfuscate_email(soup)

    orcid = ""
    orcid_link = soup.find("a", href=re.compile(r"orcid\.org"))
    if orcid_link:
        orcid = (orcid_link.get("href") or "").strip()

    website = ""
    contact_block = soup.find(class_=re.compile(r"personorganisationcontactrenderer"))
    if contact_block:
        websites_li = contact_block.find("li", class_="websites")
        if websites_li:
            a = websites_li.find("a", href=True)
            if a:
                website = (a.get("href") or "").strip()

    address = ""
    addr_block = soup.find(class_=re.compile(r"personorganisationaddressrenderer"))
    if addr_block:
        addr_div = addr_block.find(class_="address")
        if addr_div:
            address = _text(addr_div).replace("\n", " ")

    profile_text = _section_after_heading(soup, "Personal profile")
    if not profile_text and "Profile" in (soup.get_text() or ""):
        for h in soup.find_all(["h2", "h3"]):
            if "profile" in _text(h).lower():
                container = h.find_parent("section") or h.find_parent("div")
                if container:
                    profile_text = _text(container)
                break

    keywords = _section_after_heading(soup, "Keywords")

    sdgs = _section_after_heading(soup, "SDG")

    return PersonRecord(
        url=url,
        name=name,
        affiliations=affiliations,
        email=email,
        orcid=orcid,
        website=website,
        address=address,
        profile_text=profile_text,
        keywords=keywords,
        sdgs=sdgs,
    )
