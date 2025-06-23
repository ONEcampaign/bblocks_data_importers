"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.

Raw data accessed [here](https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37)

TODO: Add input cleaning for versions - HS version must be uppercase; BACI version must be lowercase
TODO: Add filtering for importer/exporter countries


"""

import pandas as pd
import os

from bblocks.data_importers.config import logger
from bblocks.data_importers.cepii.extract import BaciDataManager
from bblocks.data_importers.cepii.baci_versions import parse_baci_and_hs_versions


class BACI:
    """Importer object for the CEPII BACI international trade dataset.

    The BACI database provides highly detailed bilateral trade data across more than 200 countries and 5,000 products
    classified at the 6-digit Harmonized System (HS) level. This method allows users to easily access BACI data
    for different HS versions and BACI versions.

    Usage:

    To use this import first instantiate an object:
    >>> baci = BACI()

    This object will give you access to all available BACI versions and their supported HS classifications.
    To see the available versions and HS classifications, call the `get_available_versions()` method:
    >>> versions = baci.get_available_versions()

    This will return a dictionary with the different BACI versions available and their supported HS versions,
    as well as a bool indicator to identify the latest BACI version.

    To get the data use the `get_data()` method:
    >>> data = baci.get_data(hs_version="HS22")

    This will return a pandas DataFrame with the trade data for 2022 HS version and the latest BACI version. Different
    HS versions can be specified such as "HS22", "HS17", ... "HS92". The data includes trade values in thousands of USD
    and quantities in metric tons, with columns for exporter and importer countries, product codes, and years.
    For more information about the data call the `get_metadata()` method:

    >>> metadata = baci.get_metadata(hs_version="HS22")

    By default, the latest BACI version is used,
    but you can specify a specific version by passing the `baci_version` parameter:
    >>> data = baci.get_data(hs_version="HS22", baci_version="202401b")

    The data is cached to avoid unnecessary downloads. Because the BACI dataset is large, the data is cached to
    a temporary directory as Parquet files. The cache is deleted automatically when the object is deleted or the
    session ends. To clear the cache manually, call the `clear_cache()` method:

    >>> baci.clear_cache()

    You can filter the data for specific years or products using the `years` and `products` parameters:

    >>> data = baci.get_data(hs_version="HS22", years=2022, products=10121)

    This will return the trade data for the year 2022 and product code 10121 - "Horses: live, pure-bred
    breeding animals".

    You can also specify a list of years or products, or a range of years/products:

    >>> data = baci.get_data(hs_version="HS17", years=[2020, 2022] products=[10121, 10190])

    This will return the trade data for the years 2020 and 2022, and products 10121 and 10190.

    >>> data = baci.get_data(hs_version="HS17", years=range(2020, 2023), products=range(10121, 10190))

    This will return the trade data for the year range 2020 to 2022, and product range 10121 to 10190 (exclusive of 10190).
    Note that this uses the python `range` function, which generates a sequence of numbers from the start
    to the end value, so the end value is exclusive.

    You can specify as a tuple of two integers the start and end values for years or products, in which case the
    end value is inclusive:

    >>> data = baci.get_data(hs_version="HS17", years=(2010, 2023), products=(10121, 10190))

    This will return the trade data for the years 2010 to 2023 (inclusive) and products 10121 to 10190 (inclusive).

    You can also include country and product labels in the returned DataFrame by setting the
    `incl_country_labels` and `incl_product_labels` parameters to True:

    >>> data = baci.get_data(hs_version="HS22", incl_country_labels=True, incl_product_labels=True)

    This will return the trade data with additional columns for country names and ISO3 codes, and product descriptions.

    You can see the available product descriptions, countries, and years for a specific HS version:
    >>> product_descriptions = baci.get_product_descriptions(hs_version="HS22")
    >>> available_countries = baci.get_available_countries(hs_version="HS22")
    >>> available_years = baci.get_available_years(hs_version="HS22")

    This will return a list of product descriptions, exporter and importer country codes, names and ISO codes,
    and available years.

    To save the raw data to a local directory as a zip file, use the `save_raw_data()` method:
    >>> baci.save_raw_data(path="path/to/save/baci_data.zip", hs_version="HS22")
    """

    def __init__(self):

        # dictionary to hold loaded BACI data, keyed by version and HS version
        # Each version and HS version will be injected with a BaciDataManager instance
        self._data: dict = {}

        # dictionary to hold available BACI versions and their supported HS versions
        self._versions: dict | None = None

        # string to hold the latest BACI version
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
        products: int | list[int] | range | tuple[int, int] | None = None,
        incl_country_labels: bool = False,
        incl_product_labels: bool = False,
        baci_version: str = "latest",
    ) -> pd.DataFrame:
        """Get the BACI data.

        This method returns a pandas DataFrame with the trade data for the specified HS version and BACI version.
        By default, it returns the latest BACI version data. This method allows filtering by years and products,
        and includes options to include country and product labels in the returned DataFrame.
        The data will include trade values in thousands of USD and quantities in metric tons,
        with columns for exporter and importer countries, product codes, and years, and optionally
        country labels (names and ISO3 codes) and product descriptions.

        Args:
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            years: The year(s) to filter the data by. Defaults to None, which includes all available years.
                This can be a single year (int), a list of years, a range (e.g., range(2010, 2023)),
                or a tuple of two integers denoting the start and end years (inclusive).
            products: The product code(s) to filter the data by.
                Defaults to None, which includes all available products.
                This can be a single product code (int), a list of product codes, a range (e.g., range(10121, 10190)),
                or a tuple of two integers denoting the start and end product codes (inclusive).
            incl_country_labels: Whether to include country names and ISO3 codes in the DataFrame.
                Defaults to False.
            incl_product_labels: Whether to include product descriptions in the DataFrame.
                Defaults to False.
            baci_version: The BACI version to use. Defaults to "latest".

        Returns:
            A pandas DataFrame containing the trade data
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].get_data_frame(
            years=years,
            products=products,
            incl_country_labels=incl_country_labels,
            incl_product_labels=incl_product_labels,
        )

    def get_available_years(
        self, hs_version: str, baci_version: str = "latest"
    ) -> list[int]:
        """Get the available years for an HS version and BACI version.

        Args:
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            baci_version: The BACI version to use. Defaults to "latest".

        Returns:
            A list of available years for the specified HS version and BACI version.
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].available_years

    def get_available_countries(
        self, hs_version: str, baci_version: str = "latest"
    ) -> pd.DataFrame:
        """Get the available exporter and importer countries for an HS version and BACI version.

        Args:
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            baci_version: The BACI version to use. Defaults to "latest".

        Returns:
            A pandas DataFrame containing the available exporter and importer countries,
            with columns for country codes, names, and ISO codes.
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].country_codes

    def save_raw_data(
        self,
        path: str | os.PathLike,
        hs_version: str,
        baci_version: str = "latest",
        override: bool = False,
    ) -> None:
        """Save the raw data to disk as a zip file for a specific HS version and BACI version.

        Args:
            path: The path to save the raw data to. This should be a valid file path where the zip file will be saved,
                and a file name with a .zip extension.
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            baci_version: The BACI version to use. Defaults to "latest".
            override: Whether to override the existing file if it already exists. Defaults to False.
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        # Save the raw data to a local directory
        self._data[baci_version][hs_version].save_zip_file(path=path, override=override)

    def get_metadata(self, hs_version: str, baci_version: str = "latest") -> dict:
        """Get the metadata for a specific HS version and BACI version.

        Args:
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            baci_version: The BACI version to use. Defaults to "latest".

        Returns:
            A dictionary containing metadata for the specified HS version and BACI version
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].metadata

    def get_product_descriptions(
        self, hs_version: str, baci_version: str = "latest"
    ) -> pd.DataFrame:
        """Get the product descriptions for a specific HS version and BACI version.

        Args:
            hs_version: The Harmonized System version to use (e.g., "HS22", "HS17").
            baci_version: The BACI version to use. Defaults to "latest".

        Returns:
            A pandas DataFrame containing the product codes and their descriptions.
        """

        # Load the data if not already loaded
        self._load_data(baci_version=baci_version, hs_version=hs_version)

        if baci_version == "latest":
            baci_version = self._latest_version

        return self._data[baci_version][hs_version].product_codes

    def clear_cache(self) -> None:
        """Clear cached data"""

        self._data = {}
        self._versions = None
        self._latest_version = None
        logger.info("Cache cleared.")
