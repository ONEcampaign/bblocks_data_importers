"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found [here](https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37)

This importer provides functionality to easily access the latest BACI data (or data from a specific version),
automatically download and extract data if not already available locally, and return formatted trade data.

Usage:

>>> import bblocks.data_importers as bbdata

First, initiate a BACI object. You must specify a path to save the data locally. You may also specify a BACI version
with baci_version. If not specified, the object will be set to the latest BACI version available. You can specify an
HS classification. Note that hs_version determines how far back in time the data goes. For example, the default value
"22" returns data from 2022 onward.
>>> baci = bbdata.BACI(
...     data_path="my/local/folder",
...     baci_version="latest",
...     hs_version="22",
... )

If you would like to explore older BACI versions, the `get_versions()` method returns a dictionary with the different
BACI versions available and their supported HS versions, as well as bool indicator to identify the latest BACI version.
>>> versions = bbdata.get_baci_versions()

Get the BACI data with the `get_data()` method. The function will look for a folder of the format 'BACI_HSXX_V20XXX'
in the specified data_path, and download if not found.

You can indicate whether to include country names in the final DataFrame (defaults to True) or filter the years included
in the data.
>>> data = baci.get_data(
...     include_country_names=True,
...     years=range(2022, 2024)
... )

The traded amounts are specified in columns `value` (current thousand USD) and `quantity` (metric tons).

A dictionary that maps HS codes to product desriptions is available with:
>>> hs_map = baci.get_hs_map()

To access metadata from a BACI object:
>>> metadata = baci.get_metadata()

The data and metadata are cached to avoid loading the dataset again. Use the `clear_cache()` method to delete this
data. You can set clear_disk = True to delete the local directory where the BACI data was saver (defaults to False).
>>> baci.clear_cache(clear_disk=True)
"""

import pandas as pd

from bblocks.data_importers.config import logger, DataExtractionError
from bblocks.data_importers.cepii.extract import BaciDataManager
from bblocks.data_importers.cepii.baci_versions import parse_baci_and_hs_versions


class BACI:
    """
    Importer for the CEPII BACI international trade dataset.

    The BACI database provides highly detailed bilateral trade data across more than 200 countries and 5,000 products
    classified at the 6-digit Harmonized System (HS) level. This class provides methods to download, extract, cache,
    and structure BACI trade data, along with associated metadata and product descriptions.

    Features:
        - Download and process BACI data from the CEPII repository.
        - Automatically detect and use the latest available BACI version.
        - Support for multiple HS classifications.
        - Filter datasets by year and include country names if desired.
        - Cache formatted DataFrames and metadata to avoid redundant processing.
        - Export mappings of HS product codes to descriptions.

    Attributes:
        _data_path (Path): Local root path where BACI data is stored.
        _baci_version (str): Version string of the BACI release in use.
        _hs_version (str): Selected Harmonized System code version (e.g. "22" for HS2022).
        _data_directory (str): Folder name where BACI data is expected (e.g. 'BACI_HS22_V202501').
        _extract_path (Path): Full path to the directory where the BACI data is extracted.
        _include_country_names (bool): Whether to include country names in final DataFrame.
        _data (pd.DataFrame | None): Cached BACI data.
        _metadata (dict | None): Cached metadata extracted from the dataset's Readme.txt.
        _loaded_years (set[int] | None): Years included in the current `_data` cache.

    Usage:

        # Initiate the object by specifying directory where the data will be downloaded
        >>> baci = bbdata.BACI(data_path="my/local/folder", baci_version="latest", hs_version="22")

        # To check the available BACI and HS versions, use the `get_versions()` method:
        >>> versions = bbdata.get_baci_versions()

        # Get data as a DataFrame, specifying where to include country names and filtering specific years
        # The traded amounts are specified in columns `value` (current thousand USD) and `quantity` (metric tons).
        >>> df = baci.get_data(include_country_names=True, years=range(2022, 2024))

        # Access metadata and HS code to product description map.
        >>> metadata = baci.get_metadata()
        >>> hs_map = baci.get_hs_map()

        # Clear cache and delete local files
        >>> baci.clear_cache(clear_disk=True)
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

        logger.info("Loading available BACI versions and HS classifications...")

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
        baci_version: str = "latest",
    ) -> pd.DataFrame:
        """Get the BACI data.

        """
        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].data.to_pandas(types_mapper=pd.ArrowDtype)

    def get_metadata(self, hs_version: str, version: str = "latest") -> dict:
        """Get the BACI metadata
        """

        # Load the data if not already loaded
        self._load_data(baci_version=version, hs_version=hs_version)

        if version == "latest":
            version = self._latest_version

        return self._data[version][hs_version].metadata

    def clear_cache(self):
        """Clear cached data"""

        self._data = {}
        self._versions = None
        self._latest_version = None
        logger.info("Cache cleared.")
