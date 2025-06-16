"""Web scraping module to fetch BACI versions and HS classifications.

This module scrapes the CEPII BACI page to extract the latest and archived
BACI versions along with their associated HS classifications.

The `parse_baci_and_hs_versions` function returns a dictionary
with BACI versions as keys and their HS classifications as values.
"""

import bs4
import requests
from bs4 import BeautifulSoup
import re

from bblocks.data_importers.config import logger


BASE_CEPII_URL = "https://www.cepii.fr"
BACI_URL = BASE_CEPII_URL + "/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"


def _get_soup() -> BeautifulSoup:
    """Request the CEPII BACI page and return a BeautifulSoup object."""

    try:
        logger.debug(f"Fetching soup for BACI page")
        response = requests.get(BACI_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch BACI page: {e}")

    return BeautifulSoup(response.content, "html.parser")


def _extract_section_div(soup: BeautifulSoup, section_title: str) -> bs4.Tag:
    """Extract the contents of a specific section <div> from the BACI page."""
    # get a list of all section divs
    section_divs = soup.find_all("div", {"class": "titre-rubrique"})

    # loop through the divs to find the one with the matching title
    for div in section_divs:
        if div.get_text() == section_title:
            section_div = div

            # return the parent div that contains the section content
            return section_div.parent
    else:
        raise ValueError(f"Section '{section_title}' not found in BACI page.")


def _parse_latest_version(latest_div: bs4.Tag) -> dict:
    """Get the latest BACI version"""

    # Extract the div containing the latest version and its HS versions
    # div = _extract_section_div(soup, "Download")

    # Get the latest version from the div text
    match = re.search(
        r"This is the\s+([A-Za-z0-9]+)\s+version", latest_div.text, re.IGNORECASE
    )
    if match:
        version = match.group(1)
    else:
        raise ValueError("Latest version could not be found")

    # Extract all HS versions from the div text
    hs_versions = re.findall(r"\bHS\d{2}\b", latest_div.text)

    if not hs_versions:
        raise ValueError("No HS versions found in the latest version section.")

    return {version: {"hs_versions": hs_versions, "latest": True}}


def _parse_archive_versions(archive_div: bs4.Tag) -> dict[int, list[str]]:
    """ """

    # Step 1: Split by version headers
    blocks = re.split(r"\n+(?=\d{6}[a-z]?\s+version:)", archive_div.text)

    # Step 2: Parse each block
    version_dict = {}

    for block in blocks:
        header_match = re.match(r"(\d{6}[a-z]?)\s+version:", block)
        if header_match:
            version = header_match.group(1)
            hs_versions = re.findall(r"\bHS\d{2}\b", block)
            version_dict[version] = {"hs_versions": hs_versions}

    if not version_dict:
        raise ValueError("No archive BACI versions found")

    return version_dict


def parse_baci_and_hs_versions() -> dict:
    """Parse version declarations and associated HS versions."""

    soup = _get_soup()

    # get the latest version and hs versions
    latest_div = _extract_section_div(soup, "Download")
    latest_version = _parse_latest_version(latest_div)

    # get the archive versions and hs versions
    archive_div = _extract_section_div(soup, "Archives")
    archive_versions = _parse_archive_versions(archive_div)

    return {**latest_version, **archive_versions}
