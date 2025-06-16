"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found [here](https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37)

This importer provides functionality to easily access the latest BACI data (or data from a specific version),
automatically download and extract data if not already available locally, and return formatted trade data.

"""

import pandas as pd

from bblocks.data_importers.config import logger
from bblocks.data_importers.cepii.extract import BaciDataManager
from bblocks.data_importers.cepii.baci_versions import parse_baci_and_hs_versions


class BACI:
    """
    Importer for the CEPII BACI international trade dataset.

    The BACI database provides highly detailed bilateral trade data across more than 200 countries and 5,000 products
    classified at the 6-digit Harmonized System (HS) level. This class provides methods to download, extract, cache,
    and structure BACI trade data, along with associated metadata and product descriptions.

    """

    def __init__(self):

        self._data: dict = {}
        self._versions: dict | None = None
        self._latest_version: str | None = None

    def __repr__(self) -> str:
        """String representation of the BACI object"""
        loaded_versions = {}
        if self._data:
            for version, hs_versions in self._data.items():
                loaded_versions[version] = list(hs_versions.keys())

        latest_version_text = ""
        if self._latest_version:
            latest_version_text = f", latest_version={self._latest_version!r}"


        return (
            f"{self.__class__.__name__}("
            f"loaded_versions={loaded_versions!r}"
            f"{latest_version_text}"
            f")"
        )


    def get_available_versions(self):
        """Get available BACI versions

        This method returns a dictionary with the different BACI versions available and their supported HS versions,
        as well as a bool indicator to identify the latest BACI version.

        """

        if not self._versions:
            self._load_versions()

        return self._versions

    def _load_versions(self):
        """Load available BACI versions and HS classifications."""

        logger.info("Finding available BACI versions and HS classifications")

        self._versions = parse_baci_and_hs_versions()

        # set the latest version
        for k, v in self._versions.items():
            # if latest key in v, return v
            if "latest" in v and v["latest"]:
                self._latest_version = k
                break

    def _load_data(self, baci_version: str, hs_version: str) -> None:
        """Load BACI data to the object"""

        # TODO: clean version and hs_version inputs

        # load available versions if not already loaded
        if not self._versions:
            self._load_versions()

        # if the version is not set or is set to "latest", use the latest version available
        if baci_version == "latest":
            baci_version = self._latest_version

        # check if the version and hs_version are valid
        if baci_version not in self._versions:
            raise ValueError(
                f"{baci_version} is not a valid BACI version. Call `get_available_versions()` to see available versions,"
                f" or use 'latest' to get the most recent version."
            )
        if hs_version not in self._versions[baci_version]["hs_versions"]:
            raise ValueError(
                f"{hs_version} is not a valid HS version for BACI version {baci_version}."
                f"Available HS versions for BACI version {baci_version} are: {self._versions[baci_version]['hs_versions']}"
            )

        # if data for the version and hs_version is not loaded, load it
        if baci_version in self._data:
            if hs_version in self._data[baci_version]:
                return

        # Create a BaciDataManager instance to handle data extraction
        baci_data_manager = BaciDataManager(
            version=baci_version,
            hs_version=hs_version,
        )

        # Load the data
        baci_data_manager.load_data()

        # Store the loaded data in the object
        if baci_version not in self._data:
            self._data[baci_version] = {hs_version: baci_data_manager}
        else:
            self._data[baci_version][hs_version] = baci_data_manager

    def get_data(
        self,
        hs_version: str,
        years: int | list[int] | range | tuple[int, int] | None = None,
        baci_version: str = "latest",
    ) -> pd.DataFrame:
        """Get the BACI data.

        """
        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].get_data_frame(years=years)

    def get_available_years(self, hs_version: str, baci_version: str = "latest") -> list[int]:
        """Get the available years for an HS version and BACI version."""

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].available_years


    def get_metadata(self, hs_version: str, version: str = "latest") -> dict:
        """Get the BACI metadata
        """

        # Load the data if not already loaded
        self._load_data(baci_version=version, hs_version=hs_version)

        if version == "latest":
            version = self._latest_version

        return self._data[version][hs_version].metadata

    def get_product_descriptions(self, hs_version: str, version: str = "latest"):
        """Get the product descriptions for a specific HS version and BACI version."""

        # Load the data if not already loaded
        self._load_data(baci_version=version, hs_version=hs_version)

        if version == "latest":
            version = self._latest_version

        return self._data[version][hs_version].product_codes

    def clear_cache(self):
        """Clear cached data"""

        self._data = {}
        self._versions = None
        self._latest_version = None
        logger.info("Cache cleared.")
