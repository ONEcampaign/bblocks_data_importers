"""Web scraping module to fetch BACI versions and HS classifications.

This module scrapes the CEPII BACI webpage to extract the latest and archived
BACI versions along with their associated HS classifications.

The `parse_baci_and_hs_versions` function returns a dictionary
with BACI versions as keys and their HS classifications as values.
"""

import re
import requests
import bs4
from bs4 import BeautifulSoup

from bblocks.data_importers.config import logger


BACI_URL = "https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html"


def _get_soup() -> BeautifulSoup:
    """Request the CEPII BACI webpage and return a BeautifulSoup object."""

    try:
        logger.debug("Fetching soup for BACI page")
        response = requests.get(BACI_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch BACI page: {e}")

    return BeautifulSoup(response.content, "html.parser")


def _parse_latest_version(download_section: bs4.Tag) -> dict:
    """Get the latest BACI version from the download-links section."""

    match = re.search(
        r"This is the\s+([A-Za-z0-9]+)\s+version", download_section.text, re.IGNORECASE
    )
    if not match:
        raise ValueError("Latest version could not be found")
    version = match.group(1)

    hs_versions = [
        a.text.strip()
        for a in download_section.find_all("a")
        if re.search(r"BACI_HS\d{2}_V", a.get("href", ""))
    ]
    if not hs_versions:
        raise ValueError("No HS versions found in the latest version section.")

    return {version: {"hs_versions": hs_versions, "latest": True}}


def _parse_archive_versions(archive_section: bs4.Tag) -> dict:
    """Parse archive BACI versions from the tab-based archives section."""

    nav_links = archive_section.find_all("a", class_="nav-link")
    tab_panels = archive_section.find_all("div", class_="tab-pane")

    version_dict = {}
    for nav_link, tab_panel in zip(nav_links, tab_panels):
        version = nav_link.text.strip()
        hs_versions = [
            a.text.strip()
            for a in tab_panel.find_all("a")
            if re.search(r"BACI_HS\d{2}_V", a.get("href", ""))
        ]
        version_dict[version] = {"hs_versions": hs_versions}

    if not version_dict:
        raise ValueError("No archive BACI versions found")

    return version_dict


def parse_baci_and_hs_versions() -> dict:
    """Parse version declarations and associated HS versions from the BACI webpage.

    Returns:
        A dictionary mapping BACI version strings to a dict with:
        - "hs_versions": list of available HS version strings (e.g. ["HS92", "HS22"])
        - "latest": True only for the current latest version (absent for archive versions)
    """

    soup = _get_soup()

    download_section = soup.find("section", {"id": "download-links"})
    if download_section is None:
        raise ValueError("Download links section not found in BACI page.")
    latest_version = _parse_latest_version(download_section)

    archive_section = soup.find("section", {"id": "archives"})
    if archive_section is None:
        raise ValueError("Archives section not found in BACI page.")
    archive_versions = _parse_archive_versions(archive_section)

    return {**latest_version, **archive_versions}
