"""Importer for the GHED database from WHO"""

import pandas as pd
import requests
import io
import numpy as np
from requests.exceptions import RequestException
from pathlib import Path

from bblocks_data_importers.config import logger, DataExtractionError, DataFormattingError
from bblocks_data_importers.protocols import DataImporter

URL: str = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


class GHED(DataImporter):
    """Importer for the GHED database from WHO.

    The Global Health Expenditure Database (GHED) provides comparable data on health expenditure
    across various countries and years. See more details and access the raw data at: https://apps.who.int/nha/database

    This importer provides functionality to easily get data and metadata from the GHED database.

    Usage:

    First instantiate an importer object:
    >>> ghed = GHED()

    get the data and metadata:
    >>> data = ghed.get_data()
    >>> metadata = ghed.get_metadata()

    Download the raw data to disk:
    >>> ghed.download_raw_data(path = "some_path")


    This object caches the data as objects attributes to avoid multiple requests to the database.
    You can clear the cached data using the `clear_cache method`
    >>> ghed.clear_cache()
    """

    def __init__(self, data_file: str | None = None):
        self._raw_data: io.BytesIO | None = None
        self._data: pd.DataFrame | None = None
        self._metadata: pd.DataFrame | None = None

        self._data_file = Path(data_file) if data_file else None

        # if the data file is passed and the filed does not exist, raise an error
        if self._data_file and not self._data_file.exists():
            raise FileNotFoundError(f"The file path `{self._data_file}` does not exist. Please provide a valid file "
                                    f"path.")

    @staticmethod
    def _extract_raw_data() -> io.BytesIO:
        """Extract the raw data from the GHED database

        Returns:
            A BytesIO object containing the raw data from the GHED database
        """

        logger.info("Extracting data from GHED database")

        try:
            response = requests.get(URL)
            response.raise_for_status()  # Raises an error for HTTP codes 4xx/5xx
            return io.BytesIO(response.content)

        except RequestException as e:
            raise DataExtractionError(f"Error extracting data: {e}")

    @staticmethod
    def _read_local_data(path: Path) -> io.BytesIO:
        """Read the data from a local file

        Args:
            path: Path to the file to read

        Returns:
            A BytesIO object containing the data from the file
        """

        try:
            with path.open("rb") as f:
                return io.BytesIO(f.read())
        except Exception as e:
            raise DataExtractionError(f"Error reading data from file {path}: {e}") from e

    @staticmethod
    def _format_data(raw_data: io.BytesIO) -> pd.DataFrame:
        """Format the raw data from the GHED database

        Args:
            raw_data: Raw data extracted from the GHED database. Use the `_extract_raw_data` method to get this data

        Returns:
            A DataFrame with the formatted data
        """

        try:
            data_df = (
                pd.read_excel(raw_data, sheet_name="Data")
                .drop(columns=["region", "income"])
                .melt(id_vars=["country", "code", "year"], var_name="indicator_code")
                .rename(columns={"country": "country_name", "code": "iso3_code"})
            )

            codes_df = (
                pd.read_excel(raw_data, sheet_name="Codebook")
                .rename(
                    columns={
                        "variable code": "indicator_code",
                        "variable name": "indicator_name",
                    }
                )
                .loc[:, ["indicator_code", "indicator_name", "unit", "currency"]]
                .replace("-", np.nan)
            )

            return pd.merge(data_df, codes_df, on="indicator_code", how="left")

        except (ValueError, KeyError) as e:
            raise DataFormattingError(f"Error formatting data: {e}")

    @staticmethod
    def _format_metadata(raw_data: io.BytesIO) -> pd.DataFrame:
        """Format the metadata from the GHED database

        Args:
            raw_data: Raw data extracted from the GHED database. Use the `_extract_raw_data` method to get this data

        Returns:
            A DataFrame with the formatted metadata
        """

        cols = {
            "country": "country_name",
            "code": "iso3_code",
            "variable name": "indicator_name",
            "variable code": "indicator_code",
            "Sources": "sources",
            "Comments": "comments",
            "Data type": "data_type",
            "Methods of estimation": "methods_of_estimation",
            "Country footnote": "country_footnote",
        }

        try:
            return (
                pd.read_excel(raw_data, sheet_name="Metadata")
                .rename(columns=cols)
                .loc[:, cols.values()]
            )

        except (ValueError, KeyError) as e:
            raise DataFormattingError(f"Error formatting metadata: {e}")

    def _load_data(self) -> None:
        """Load the data from the GHED database to the object"""

        if self._data_file:
            logger.info(f"Loading data from local file")
            self._raw_data = self._read_local_data(self._data_file)
        else:
            self._raw_data = self._extract_raw_data()

        self._data = self._format_data(self._raw_data)
        self._metadata = self._format_metadata(self._raw_data)

    def get_data(self) -> pd.DataFrame:
        """Get the data from the GHED database

        # TODO: Add functionality to filter the data by country, indicator, year, etc.

        Returns:
            A DataFrame with the data from the GHED database
        """

        if self._data is None:
            self._load_data()

        return self._data

    def get_metadata(self) -> pd.DataFrame:
        """Get the metadata for the GHED database

        Returns:
            A DataFrame with the metadata for the GHED database including sources, footnotes, comments, etc.
            for each indicator-country pair
        """

        if self._metadata is None:
            self._load_data()

        return self._metadata

    def clear_cache(self) -> None:
        """Clear the data cached in the importer"""

        self._raw_data = None
        self._data = None
        self._metadata = None
        logger.info("Cache cleared")

    def download_raw_data(self, path: str, file_name="ghed", overwrite=False) -> None:
        """Download the raw data to disk.

        This method saves the raw data to disk in the specified path as an Excel file.

        Args:
            path: Path to the directory where the data will be saved
            file_name: Name of the file to save the data
            overwrite: Whether to overwrite the file if it already exists. Default is False
        """

        directory = Path(path)
        if not directory.exists():
            raise FileNotFoundError(
                f"The directory `{directory}` does not exist. Please provide a valid directory."
            )
        file_path = directory / f"{file_name}.xlsx"
        if file_path.exists() and not overwrite:
            raise FileExistsError(
                f"The file `{file_path}` already exists. Set overwrite=True to overwrite."
            )

        if self._raw_data is None:
            self._load_data()

        with open(file_path, "wb") as file:
            file.write(self._raw_data.getvalue())
        logger.info(f"Data saved to `{file_path}`")
