from dataclasses import dataclass


@dataclass
class PersonRecord:
    url: str
    name: str
    affiliations: str
    email: str
    orcid: str
    website: str
    address: str
    profile_text: str
    keywords: str
    sdgs: str

    def to_row(self) -> dict:
        return {
            "url": self.url,
            "name": self.name,
            "affiliations": self.affiliations,
            "email": self.email,
            "orcid": self.orcid,
            "website": self.website,
            "address": self.address,
            "profile_text": self.profile_text,
            "keywords": self.keywords,
            "sdgs": self.sdgs,
        }
