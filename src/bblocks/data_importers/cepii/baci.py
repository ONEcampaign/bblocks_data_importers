"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found at: https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37

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

The data and metadata are cached to avoid loading the dataset again. User the `clear_cache()` method to delete this
data. You can set clear_disk = True to delete the local directory where the BACI data was saver (defaults to False).
>>> baci.clear_cache(clear_disk=True)
"""

import io
import pandas as pd
import requests
from typing import Literal
from pathlib import Path
import shutil

from bblocks.data_importers.config import logger, Fields
from bblocks.data_importers.protocols import DataImporter
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.cepii.static_methods import (
    get_available_versions,
    extract_zip,
    rename_columns,
    map_country_codes,
    organise_columns,
    combine_data,
    save_parquet,
    load_parquet,
    cleanup_csvs,
    generate_metadata,
    validate_years,
)

BASE_URL = "https://www.cepii.fr/DATA_DOWNLOAD/baci/data"

VERSIONS_DICT = get_available_versions()
VALID_BACI_VERSIONS = VERSIONS_DICT.keys()


def get_baci_versions() -> dict[str, dict[str, list[int] or bool]]:
    """Returns a dictionary with the different BACI versions available and their supported HS versions, as well as
    bool indicator to identify the latest BACI version.

    Returns:
        dict: Dictionary mapping BACI to HS versions and latest flag.
    """
    return VERSIONS_DICT


class BACI(DataImporter):
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
        >>> import bblocks.data_importers as bbdata
        >>> # Initiate the object by specifying directory where the data will be downloaded
        >>> baci = bbdata.BACI(data_path="my/local/folder", baci_version="latest", hs_version="22")
        >>> # To check the available BACI and HS versions, use the `get_versions()` method:
        >>> versions = bbdata.get_baci_versions()
        >>> # Get data as a DataFrame, specifying where to include country names and filtering specific years
        >>> # The traded amounts are specified in columns `value` (current thousand USD) and `quantity` (metric tons).
        >>> df = baci.get_data(include_country_names=True, years=range(2022, 2024))
        >>> # Access metadata and HS code to product description map.
        >>> metadata = baci.get_metadata()
        >>> hs_map = baci.get_hs_map()
        >>> # Clear cache and delete local files
        >>> baci.clear_cache(clear_disk=True)
    """

    def __init__(
        self,
        data_path: Path | str,
        baci_version: Literal[
            "202102", "202201", "202301", "202401", "202401b", "202501", "latest"
        ] = "latest",
        hs_version: Literal["92", "96", "02", "07", "12", "17", "22"] = "22",
    ):
        """Initialize a BACI importer instance.

        Args:
            data_path: Base path to extract downloaded data.
            baci_version: BACI version to use (default: 'latest').
            hs_version: HS version to use (default: '22').
        """

        # Validate data path
        if data_path is None:
            raise ValueError("`data_path` must be defined.")

        path = Path(data_path)

        if not path.exists():
            raise FileNotFoundError(
                f"The path `{path}` does not exist. Please provide a valid directory."
            )

        self._data_path = path

        # validate BACI version
        if baci_version == "latest":
            self._baci_version = next(
                v for v, d in VERSIONS_DICT.items() if d["latest"]
            )
        else:
            if baci_version not in VALID_BACI_VERSIONS:
                raise ValueError(
                    f"Unsupported BACI version: {baci_version}. Available versions: {list(VALID_BACI_VERSIONS) + ['latest']}"
                )
            else:
                self._baci_version = baci_version

        # Validate HS version
        valid_hs_version = VERSIONS_DICT[self._baci_version]["hs"]
        if hs_version not in valid_hs_version:
            raise ValueError(
                f"Invalid HS version: {hs_version}. Available versions for BACI {self._baci_version}: {valid_hs_version}"
            )
        else:
            self._hs_version = hs_version

        self._data_directory = f"BACI_HS{self._hs_version}_V{self._baci_version}"
        self._extract_path = self._data_path / self._data_directory

        self._include_country_names: bool = True

        self._data: pd.DataFrame | None = None
        self._metadata: dict | None = None
        self._loaded_years: set[int] | None = None

    def _download_zip(self) -> io.BytesIO:
        """Download ZIP file from the BACI database."""
        download_url = f"{BASE_URL}/{self._data_directory}.zip"
        logger.info("Downloading BACI data. This may take a while...")

        response = requests.get(download_url)
        response.raise_for_status()

        return io.BytesIO(response.content)

    def _load_country_codes(self) -> pd.DataFrame:
        """Load the country code mapping CSV."""
        path = self._extract_path / f"country_codes_V{self._baci_version}.csv"
        return pd.read_csv(path)

    def _format_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Orchestrates formatting of raw BACI data by 1) renaming, 2) mapping BACI country codes to ISO-3 codes and
        country names and 3) ordering columns and dropping unnecessary ones.

        Args:
            raw_df: Raw consolidated BACI DataFrame.

        Returns:
            Formatted DataFrame.
        """
        country_codes_df = self._load_country_codes()
        df = rename_columns(raw_df)
        df = map_country_codes(df, country_codes_df, self._include_country_names)
        df = organise_columns(df)

        return convert_dtypes(df)

    def _ensure_parquet_data_exists(self):
        """Ensure the Parquet data directory exists; if not, download and process the raw ZIP."""
        parquet_dir = self._extract_path / "parquet"

        if parquet_dir.exists() and any(parquet_dir.rglob("*.parquet")):
            return parquet_dir

        zip_file = self._download_zip()
        self._extract_path.mkdir(parents=True, exist_ok=True)
        extract_zip(zip_file, self._extract_path)

        table = combine_data(self._extract_path)
        save_parquet(table, parquet_dir)

        cleanup_csvs(self._extract_path)

        return parquet_dir

    def _load_data(self, filter_years: set[int] | None):
        """Load, format, and cache BACI data for the given filter years."""
        parquet_dir = self._ensure_parquet_data_exists()
        filter_years = validate_years(parquet_dir, filter_years)

        logger.info(f"Loading consolidated BACI dataset from {parquet_dir}")
        raw_df = load_parquet(parquet_dir, filter_years)

        df = self._format_data(raw_df)

        required_cols = [
            Fields.year,
            Fields.exporter_iso3,
            Fields.importer_iso3,
            Fields.product_code,
            Fields.value,
            Fields.quantity,
        ]

        if self._include_country_names:
            required_cols += [Fields.exporter_name, Fields.importer_name]

        DataFrameValidator().validate(df, required_cols=required_cols)

        self._data = df
        self._loaded_years = filter_years
        logger.info("Data loaded successfully")

    def get_data(
        self,
        include_country_names: bool = True,
        years: int | list[int] | range | set[int] | None = None,
        force_reload: bool = False,
    ) -> pd.DataFrame:
        """Get the BACI data

        This method will return a DataFrame with BACI data. The returned DataFrame includes ISO-3 codes for importer and
        exporter countries and optionally, country names as well. The traded amounts are specified in columns `value`
        (current thousand USD) and `quantity` (metric tons).

        Arguments:
            include_country_names (bool): Whether to include a column with country names in addition to ISO-3 codes
            (default: True).
            years (int | list[int] | range | set[int] | None): Years to include in the data (default: None, which returns all
            available years).
            force_reload (bool): Whether to fetch new data even if it already exists (default: False).

        Returns:
            pd.DataFrame: BACI trade data
        """

        # Normalize years as a set for later operations
        if years is not None:
            if isinstance(years, int):
                years = {years}
            else:
                years = set(years)

        config_changed = (
            self._include_country_names != include_country_names
            or self._loaded_years != years
        )

        if self._data is None or config_changed or force_reload:
            self._include_country_names = include_country_names
            self._load_data(filter_years=years)

        return self._data

    def _extract_hs_map(self) -> dict[str, str]:
        """Extract HS map from product_codes_HSXX_XXXX.csv if present."""

        file_name = f"product_codes_HS{self._hs_version}_V{self._baci_version}.csv"
        file_path = self._extract_path / file_name
        parquet_path = self._extract_path / "parquet"

        if not file_path.exists():
            if parquet_path.exists():
                raise FileNotFoundError(
                    f"HS map file {file_name} not found in {self._extract_path}. "
                    "You may have an incomplete local dataset.\n"
                    f"To rebuild it with metadata, try running `clear_cache(clean_disk=True)` followed by `get_data()`"
                )
            else:
                logger.warning("BACI files not found locally.")
                self.get_data()

        product_codes_df = pd.read_csv(
            file_path,
            dtype={"code": str},
        )
        product_dict = dict(
            zip(product_codes_df["code"], product_codes_df["description"])
        )

        return product_dict

    def get_hs_map(self, force_reload: bool = False) -> dict[str, str]:
        """Get a dictionary mapping HS codes to product descriptions.

        Arguments:
            force_reload (bool): Whether to fetch new data even if it already exists (default: False).

        Returns:
            dict: Map of HS codes to products.
        """

        if force_reload:
            self.get_data()

        return self._extract_hs_map()

    def _extract_metadata(self):
        """Extract metadata from Readme.txt if present."""

        readme_path = self._extract_path / "Readme.txt"
        parquet_path = self._extract_path / "parquet"

        if not readme_path.exists():
            if parquet_path.exists():
                raise FileNotFoundError(
                    f"Metadata file 'Readme.txt' not found in {self._extract_path}. "
                    "You may have an incomplete local dataset.\n"
                    f"To rebuild it with metadata, try running `clear_cache(clean_disk=True)` followed by `get_data()`"
                )
            else:
                logger.warning("BACI files not found locally.")
                self.get_data()

        self._metadata = generate_metadata(readme_path)

    def get_metadata(self, force_reload: bool = False) -> dict:
        """Get the BACI metadata

        Returns a dictionary with BACI metadata including version, release data, weblink, and more.

        Arguments:
            force_reload (bool): Whether to fetch new data even if it already exists. (default: False).

        Returns:
            dict: BACI metadata.
        """

        if force_reload:
            self.get_data()

        if self._metadata is None:
            self._extract_metadata()

        return self._metadata

    def clear_cache(self, clear_disk: bool = False):
        """Clear cached data.

        This includes the DataFrame returned by `get_data()` and its metadata. If clear_disk is True, the local
        directory where the BACI data was extracted will be deleted.

        Arguments:
             clear_disk (bool): Whether to delete local data directory (default: False).
        """

        self._data = None
        self._metadata = None

        if clear_disk and self._extract_path.exists():
            shutil.rmtree(self._extract_path)
            logger.info(f"Deleting local BACI directory: {self._extract_path}")

        logger.info("Cache cleared")
