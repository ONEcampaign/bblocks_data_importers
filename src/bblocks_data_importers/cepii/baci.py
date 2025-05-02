"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found at: https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37

This importer provides functionality to easily access the latest BACI data (or data from a specific version),
automatically download and extract data if not already available locally, and return formatted trade data.

Usage:

First instantiate an importer object:
>>> baci = BACI(data_path="my/local/folder")

Get the latest BACI data with. The function will look for a folder of the format 'BACI_HSXX_V20XXX' in the
specified data_path, and download if not found:
>>> data = baci.get_data()

You can specify a BACI version and an HS classification.
Note: The hs_version determines how far back in time the data goes.
For example, the default value "22" returns data from 2022 onward.
You can also choose the country code format and whether to include product code descriptions in the output.
>>> baci = BACI(
    data_path="my/local/folder"
    baci_version="202401",
    hs_version="22",
    country_format="iso3",
    product_description=False
    )

To access metadata from a BACI object:
>>> metadata = baci.get_metadata()

The data and metadata are cached to avoid building the dataset again. Clear the cache with:
>>> baci.clear_cache()
"""

import io
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import zipfile
from typing import Literal
from pathlib import Path

from bblocks_data_importers.config import (
    logger,
    Fields,
)
from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes
from bblocks_data_importers.data_validators import DataFrameValidator


class BACI(DataImporter):
    """
    Importer class for BACI trade data from CEPII.

    Attributes:
        _data_path (Path): Local path where data is stored or downloaded.
        _baci_version (str): BACI dataset version.
        _hs_version (str): Harmonized System version.
        _country_format (str): Country code format (e.g., 'name', 'iso2').
        _product_description (bool): Whether to include product code description.
        _data (DataFrame | None): Cached BACI data.
        _metadata (dict | None): Cached metadata from Readme.txt.
    """

    def __init__(
        self,
        data_path: str | None = None,
        baci_version: Literal[
            "202102", "202201", "202301", "202401", "202401b", "202501", "latest"
        ] = "latest",
        hs_version: Literal["92", "96", "02", "07", "12", "17", "22"] = "22",
        country_format: Literal["code", "name", "iso2", "iso3"] = "name",
        product_description: bool = True,
    ):
        """
        Initialize a BACI importer instance.

        Args:
            data_path: Optional base path for local data. If None, will default to current directory.
            baci_version: BACI version to use (default: 'latest').
            hs_version: Harmonized System version (default: '22').
            country_format: Format for country codes: 'code', 'name', 'iso2', 'iso3' (default: 'name').
            product_description: Whether to include product code description (default: True).
        """

        self._data_path = Path(data_path) if data_path else Path(os.getcwd())
        # Raise an error if path does not exist
        if self._data_path and not self._data_path.exists():
            raise FileNotFoundError(
                f"The path `{self._data_path}` does not exist. Please provide a valid directory."
            )

        if baci_version == "latest":
            self._baci_version = self._get_latest_version()
        else:
            self._baci_version = baci_version

        self._hs_version = hs_version
        self._data_directory = Path(f"BACI_HS{self._hs_version}_V{self._baci_version}")
        self._country_format = country_format
        self._product_description = product_description

        self._data = None
        self._metadata = None

    def _get_latest_version(self):
        """
        Scrape CEPII website to get the latest BACI version.

        Returns:
            Latest BACI version string.

        Raises:
            RuntimeError: If the version cannot be determined.
        """

        url = "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", id="telechargement")

        if not div:
            raise RuntimeError(
                "Latest BACI version not found. HTML object not present in the webpage."
            )

        match = re.search(
            r"This is the\s+(\d+)\s+version", div.get_text(), re.IGNORECASE
        )
        if match:
            return match.group(1)
        else:
            raise RuntimeError("Latest BACI version not found.")

    def _get_zip_data(self):
        """
        Download and extract BACI ZIP archive into the target data path.

        Raises:
            RuntimeError: If download fails.
        """

        url = f"https://www.cepii.fr/DATA_DOWNLOAD/baci/data/{self._data_directory}.zip"
        logger.info("Downloading BACI data. This may take a while...")

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error {response.status_code}")

        zip_data = io.BytesIO(response.content)

        extract_path = self._data_path / self._data_directory

        with zipfile.ZipFile(zip_data) as zip_ref:
            zip_ref.extractall(extract_path)

        logger.info(f"BACI data extracted to: {extract_path}")

    def _load_csv(self, filename):
        """
        Load a CSV file from the BACI data directory.

        Args:
            filename: CSV file name.

        Returns:
            DataFrame loaded from the file.
        """

        path = self._data_path / self._data_directory / filename
        return pd.read_csv(path, dtype={"k": str, "code": str})

    def _format_data(self, raw_df, country_map, product_map):
        """
        Rename and map BACI raw data columns to standard schema.

        Args:
            raw_df: Raw BACI DataFrame.
            country_map: Mapping from country codes to desired format.
            product_map: Mapping from product codes to names.

        Returns:
            Formatted DataFrame.
        """

        df = raw_df.rename(
            columns={
                "t": Fields.year,
                "i": Fields.exporter,
                "j": Fields.importer,
                "k": Fields.product_code,
                "v": Fields.value,
                "q": Fields.quantity,
            }
        )

        if self._country_format != "code":
            df[Fields.exporter] = df[Fields.exporter].map(country_map)
            df[Fields.importer] = df[Fields.importer].map(country_map)

        if self._product_description:
            df[Fields.product_description] = df[Fields.product_code].map(product_map)

        df = convert_dtypes(df, "pyarrow")

        return df

    def _combine_data(self) -> pd.DataFrame:
        """
        Combine BACI CSV files for multiple years into a single DataFrame.

        Returns:
            Concatenated and formatted BACI DataFrame.
        """

        product_codes_df = self._load_csv(
            f"product_codes_HS{self._hs_version}_V{self._baci_version}.csv"
        )
        product_codes_dict = dict(
            zip(product_codes_df["code"], product_codes_df["description"])
        )

        country_codes_df = self._load_csv(f"country_codes_V{self._baci_version}.csv")
        country_codes_dict = dict(
            zip(
                country_codes_df["country_code"],
                country_codes_df[f"country_{self._country_format}"],
            )
        )

        files = os.listdir(self._data_path / self._data_directory)

        dfs = []
        
        for f in files:
            if f.startswith("BACI") and f.endswith(".csv"):
                raw_df = self._load_csv(f)
                formatted_df = self._format_data(
                    raw_df,
                    country_codes_dict,
                    product_codes_dict,
                )
                dfs.append(formatted_df)

        return pd.concat(dfs, ignore_index=True)

    def _load_data(self):
        """
        Load BACI data from local directory or download if not available.

        Validates data structure and caches the result internally.
        """

        if (self._data_path / self._data_directory).exists():
            logger.info(
                f"Importing data from local file: {self._data_path}/{self._data_directory}"
            )
        else:
            logger.info("Importing data from BACI database")
            self._get_zip_data()

        df = self._combine_data()

        required_cols = [
            Fields.year,
            Fields.exporter,
            Fields.importer,
            Fields.product_code,
            Fields.value,
            Fields.quantity,
        ]

        if self._product_description:
            required_cols.append(Fields.product_description)

        DataFrameValidator().validate(
            df,
            required_cols=required_cols,
        )

        self._data = df
        logger.info("Data imported successfully")

    def get_data(
        self,
    ) -> pd.DataFrame:
        """Get the BACI data

        This method will return a DataFrame with BACI data

        Returns:
            DataFrame with BACI data
        """

        if self._data is None:
            self._load_data()

        return self._data

    def _extract_metadata(self):
        """
        Extract metadata from the Readme.txt file.

        Populates the internal `_metadata` dictionary.
        """
        readme_path = self._data_path / self._data_directory / "Readme.txt"

        if not readme_path.exists():
            logger.info("Metadata file not found. Downloading BACI data...")
            self._get_zip_data()

        with open(readme_path, encoding="utf-8") as file:
            content = file.read()

        # Split into blocks separated by one or more blank lines
        blocks = [block.strip() for block in content.split("\n\n") if block.strip()]

        metadata = {}

        for block in blocks:
            # Skip the List of Variables section
            if block.startswith("List of Variables:"):
                continue

            # Try to split block into key and value
            lines = block.splitlines()
            if ":" in lines[0]:
                key, first_value_line = lines[0].split(":", 1)
                key = key.strip()
                value_lines = [first_value_line.strip()] + [
                    line.strip() for line in lines[1:]
                ]
                metadata[key] = " ".join(value_lines)
            else:
                # Single-line or malformed block, skip or handle as needed
                continue

        self._metadata = metadata

    def get_metadata(self) -> dict:
        """Get the BACI metadata

        Returns a dictionary with BACI metadata including version, release data, weblink, and more.

        Returns:
            The BACI metadata dictionary.
        """

        if self._metadata is None:
            self._extract_metadata()
        return self._metadata

    def clear_cache(self):
        """Clear the cached data and metadata."""

        self._data = None
        self._metadata = None

        logger.info("Cache cleared")
