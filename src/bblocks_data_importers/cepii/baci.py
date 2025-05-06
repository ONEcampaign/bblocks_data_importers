"""Importer for the BACI trade database from CEPII

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found at: https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37

This importer provides functionality to easily access the latest BACI data (or data from a specific version),
automatically download and extract data if not already available locally, and return formatted trade data.

Usage:

First, initiate a BACI object. It is recommended that you specify a path to save the data. Otherwise, the data will be
saved to the current directory.
>>> baci = BACI(data_path="my/local/folder")

Get the latest BACI data with the get_data method. The function will look for a folder of the format 'BACI_HSXX_V20XXX'
in the specified data_path, and download if not found.
You can specify country name format, whether to include descriptions for product codes (defaults to True), or filter the
years included in the data.
>>> data = baci.get_data(
...     country_format="iso3",
...     product_description=False,
...     years=[2023]
... )

You can specify a BACI version and an HS classification.
Note: The hs_version determines how far back in time the data goes.
For example, the default value "22" returns data from 2022 onward.
>>> baci = BACI(
...     data_path="my/local/folder",
...     baci_version="202401",
...     hs_version="22"
... )

To access metadata from a BACI object:
>>> metadata = baci.get_metadata()

The data and metadata are cached to avoid loading the dataset again. Clear the cache with:
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
import pyarrow as pa
import pyarrow.csv as pv
from pyarrow import dataset as ds

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
        _loaded_years (list | None):
        _metadata (dict | None): Cached metadata from Readme.txt.
    """

    def __init__(
        self,
        data_path: str | None = None,
        baci_version: Literal[
            "202102", "202201", "202301", "202401", "202401b", "202501", "latest"
        ] = "latest",
        hs_version: Literal["92", "96", "02", "07", "12", "17", "22"] = "22",
    ):
        """
        Initialize a BACI importer instance.

        Args:
            data_path: Optional base path for local data. If None, will default to current directory.
            baci_version: BACI version to use (default: 'latest').
            hs_version: Harmonized System version (default: '22').
        """

        self._data_path = Path(data_path) if data_path else Path(os.getcwd())
        # Raise an error if path does not exist
        if not self._data_path.exists():
            raise FileNotFoundError(
                f"The path `{self._data_path}` does not exist. Please provide a valid directory."
            )

        if baci_version == "latest":
            self._baci_version = self._get_latest_version()
        else:
            self._baci_version = baci_version

        self._hs_version = hs_version
        self._data_directory = Path(f"BACI_HS{self._hs_version}_V{self._baci_version}")
        self._country_format = None
        self._product_description = None

        self._data = None
        self._loaded_years = None
        self._metadata = None

    def _get_latest_version(self) -> str:
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

    def _download_and_extract(self):
        """
        Download BACI ZIP archive into the target data path.

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
        extract_path.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_data) as zip_ref:
            zip_ref.extractall(path=extract_path)

    def _format_data(self, raw_df, extract_path):
        """
        Rename and map BACI raw data columns to standard schema.

        Args:
            raw_df: Raw consolidated BACI DataFrame.
            extract_path: Path to BACI data directory.

        Returns:
            Formatted DataFrame.
        """

        product_codes_df = pd.read_csv(
            extract_path
            / f"product_codes_HS{self._hs_version}_V{self._baci_version}.csv",
            dtype={"code": str},
        )
        product_map = dict(
            zip(product_codes_df["code"], product_codes_df["description"])
        )

        country_codes_df = pd.read_csv(
            extract_path / f"country_codes_V{self._baci_version}.csv"
        )
        country_map = dict(
            zip(
                country_codes_df["country_code"],
                country_codes_df[f"country_{self._country_format}"],
            )
        )

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

        return convert_dtypes(df)

    def _combine_data(self):
        """
        Combine BACI CSV files for multiple years into a single PyArrow Table.

        Returns:
            pyarrow.Table: Consolidated BACI data.
        """

        logger.info("Building consolidated dataset")

        extract_path = self._data_path / self._data_directory
        tables = []

        column_types = {
            "t": pa.int16(),
            "i": pa.int32(),
            "j": pa.int32(),
            "k": pa.string(),
            "v": pa.float32(),
            "q": pa.float32(),
        }

        for csv_path in extract_path.glob("BACI*.csv"):
            table = pv.read_csv(
                csv_path,
                read_options=pv.ReadOptions(autogenerate_column_names=False),
                convert_options=pv.ConvertOptions(column_types=column_types),
            )
            tables.append(table)

        if not tables:
            raise FileNotFoundError("No BACI CSV files found in data directory.")

        return pa.concat_tables(tables)

    def _save_parquet(self, table: pa.Table, path: Path):
        """
        Save the provided PyArrow Table to partitioned Parquet format.

        Args:
            table (pa.Table): Consolidated trade data.
            path (Path): Destination directory for Parquet files.
        """
        logger.info(f"Saving consolidated BACI dataset to {path}")

        ds.write_dataset(
            data=table,
            base_dir=str(path),
            format="parquet",
            partitioning=["t"],
            existing_data_behavior="overwrite_or_ignore",
        )

    def _load_parquet_dataset(
        self, parquet_dir: Path, filter_years: list[int] | None = None
    ):
        dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning=["t"])

        if filter_years:
            logger.info(f"Filtering for years: {filter_years}")
            filter_expr = ds.field("t").isin(filter_years)
            table = dataset.to_table(filter=filter_expr)
        else:
            table = dataset.to_table()

        return table.to_pandas(types_mapper=pd.ArrowDtype)

    @staticmethod
    def _cleanup_csvs(path: Path):
        for f in path.glob("BACI*.csv"):
            f.unlink()

    def _load_data(self, filter_years: list[int] | None = None):
        """
        Load BACI data from local directory or download if not available.

        Validates data structure and caches the result internally.
        """

        extract_path = self._data_path / self._data_directory
        parquet_dir = extract_path / "parquet"

        if parquet_dir.exists() and any(parquet_dir.rglob("*.parquet")):
            logger.info(f"Loading consolidated BACI dataset from: {parquet_dir}")
            raw_df = self._load_parquet_dataset(
                parquet_dir, filter_years
            )  # Already a DataFrame
        else:
            self._download_and_extract()
            table = self._combine_data()  # PyArrow Table
            self._save_parquet(table, parquet_dir)
            self._cleanup_csvs(extract_path)
            raw_df = table.to_pandas(types_mapper=pd.ArrowDtype)

        df = self._format_data(raw_df, extract_path)

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

        DataFrameValidator().validate(df, required_cols=required_cols)
        self._data = df
        self._loaded_years = filter_years
        logger.info("Data loaded successfully")

    def get_data(
        self,
        country_format: Literal["code", "name", "iso2", "iso3"] = "name",
        product_description: bool = True,
        years: list[int] | None = None,
    ) -> pd.DataFrame:
        """Get the BACI data

        This method will return a DataFrame with BACI data

        Arguments:
            country_format: Format for country names: 'code', 'name', 'iso2', 'iso3' (default: 'name').
            product_description: Whether to include product code description (default: True).
            years: Years to include in the data (default: None).

        Returns:
            pd.DataFrame: BACI trade data
        """

        config_changed = (
            self._country_format != country_format
            or self._product_description != product_description
        )

        if self._data is None or config_changed or self._loaded_years != years:
            self._country_format = country_format
            self._product_description = product_description
            self._load_data(filter_years=years)

        df = self._data

        return df

    def _extract_metadata(self):
        """
        Extract metadata from the Readme.txt file.

        If the file is missing, logs a warning and skips extraction.

        Populates the internal `_metadata` dictionary if possible.
        """

        extract_path = self._data_path / self._data_directory
        readme_path = extract_path / "Readme.txt"
        parquet_path = extract_path / "parquet"

        if not readme_path.exists():
            if parquet_path.exists():
                logger.warning(
                    f"Metadata file 'Readme.txt' not found in {extract_path}. "
                    "You may have an incomplete local dataset.\n"
                    f"To rebuild it with metadata, try running `clear_cache()` followed by `get_data()` "
                    f"after deleting the file: {parquet_path.name}"
                )
                self._metadata = {}
                return
            else:
                raise FileNotFoundError(
                    f"Metadata file is missing and no data found at {extract_path}. "
                    "Please call `get_data()` first to download BACI resources."
                )

        with open(readme_path, encoding="utf-8") as file:
            content = file.read()

        # Split into blocks separated by one or more blank lines
        blocks = [block.strip() for block in content.split("\n\n") if block.strip()]
        metadata = {}

        for block in blocks:
            if block.startswith("List of Variables:"):
                continue

            lines = block.splitlines()
            if ":" in lines[0]:
                key, first_value_line = lines[0].split(":", 1)
                key = key.strip()
                value_lines = [first_value_line.strip()] + [
                    line.strip() for line in lines[1:]
                ]
                metadata[key] = " ".join(value_lines)

        self._metadata = metadata

    def get_metadata(self) -> dict:
        """Get the BACI metadata

        Returns a dictionary with BACI metadata including version, release data, weblink, and more.

        Returns:
            dict: BACI metadata.
        """

        if self._metadata is None:
            self._extract_metadata()
        return self._metadata

    def clear_cache(self):
        """Clear the cached data and metadata."""

        self._data = None
        self._metadata = None

        logger.info("Cache cleared")
