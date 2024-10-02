"""Importer for the GHED database from WHO"""


import pandas as pd
import requests
import io
import numpy as np
from requests.exceptions import RequestException

from bblocks_data_importers.config import logger, Paths
from bblocks_data_importers.protocols import DataImporter


URL = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


def extract_data() -> io.BytesIO:
    """Extract GHED dataset"""

    try:
        response = requests.get(URL)
        response.raise_for_status()  # Raises an error for HTTP codes 4xx/5xx
        return io.BytesIO(response.content)

    except RequestException as e:
        raise ConnectionError(f"Error connecting to GHED database: {e}")


def _clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED metadata"""

    to_drop = [
        "country",
        "region (WHO)",
        "Income group",
        'long code (GHED data explorer)',
        'variable name'
    ]

    return (df
            .drop(columns = to_drop)
            .rename(columns={"code": "country_code","variable code": "indicator_code",}
                    )
            )


def _clean_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED codes"""

    return df.rename(
        columns={
            "variable code": "indicator_code",
            "Indicator short code": "indicator_code",
            "variable name": "indicator_name",
            "Indicator name": "indicator_name",
            "Category 1": "category_1",
            "Category 2": "category_2",
            "Indicator units": "indicator_units",
            "Indicator currency": "indicator_currency",
        }
    ).replace({"-": np.nan})


def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED _data dataframe"""

    return df.rename(
        columns={
            "country": "country_name",
            "code": "country_code",
            "country code": "country_code",
            "income group": "income_group",
            "region (WHO)": "region",
            "region": "region",
            "income": "income_group",
        }
    ).melt(
        id_vars=["country_name", "country_code", "region", "income_group", "year"],
        var_name="indicator_code",
    )


def get_data() -> pd.DataFrame:
    """Download GHED dataset and format it"""

    data = extract_data()

    data_df = pd.read_excel(data, sheet_name="Data").pipe(_clean_data)
    codes_df = pd.read_excel(data, sheet_name="Codebook").pipe(_clean_codes)
    metadata_df = pd.read_excel(data, sheet_name="Metadata").pipe(_clean_metadata)

    df = (data_df
          .merge(codes_df, on="indicator_code", how="left")
          .merge(metadata_df, on = ['country_code', "indicator_code"], how="left")
          )

    return df


class GHEDImporter(DataImporter):
    """Importer for the GHED database

    This objects provides access to the WHO Global Health Expenditure Database (GHED) data.
    More information about the dataset can be found at https://apps.who.int/nha/database/Home/Index/en

    To get the data use the get_data method.
    If the data has never been loaded, it will be downloaded and cached (saved to disk).
    Future calls to get_data will use the cached data. New objects will also use the cached data if it is available.
    To get the data without using the cache, set use_cache to False when calling get_data.

    Optionally disable caching for the object by setting caching to False on instantiation. This will
    prevent the data from being saved to disk and the cache will not be used.
    """

    def __init__(self, caching: bool = True):

        self._data_file_path = Paths.data / "ghed_data.feather"
        self._data: pd.DataFrame | None = None
        self._caching: bool = caching

    def _load_data(self, use_cache: bool = True) -> None:
        """Load the data to the object

        If the data exists in disk and check_disk is True, load it.
        Otherwise, download the data and save it to disk and load it to the object.
        If use_cache is False or if caching is set to False on instantiation,
        do not use the cached data
        if the caching feature has been entirely disabled, the data will not be saved to disk


        Args:
            use_cache: If True, use the cached data if it exists in disk
        """

        if self._data_file_path.exists() and use_cache and self._caching:
            logger.info("Loading data from disk")
            self._data = pd.read_feather(self._data_file_path)

        else:
            logger.info("Downloading data")
            self._data = get_data()
            # if the caching feature is enabled, save the data to disk
            if self._caching:
                self._data.to_feather(self._data_file_path)
                logger.info("Data saved to disk")

    def get_data(self, metadata: bool = False, use_cache: bool = True) -> pd.DataFrame:
        """Get GHED data

        If the data has never been loaded, it will be downloaded and saved to disk.
        If the data has been loaded before, it will be loaded from disk.
        To get the data without using the cache, set use_cache to False. This will
        force the data to be downloaded from the GHED database.

        Args:
            metadata: If True, return the data including metadata
                        Default is False
            use_cache: If True, use the cached data if it exists

        Returns:
            GHED data as a pandas DataFrame
        """

        if self._data is None:
            self._load_data(use_cache)

        if not metadata:
            return self._data.loc[:, ['country_name', 'country_code',
                                      'indicator_code', 'indicator_name', 'year',
                                      'value']]

        return self._data

