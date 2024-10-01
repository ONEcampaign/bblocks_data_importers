"""Importer for the GHED database from WHO"""


import pandas as pd
import requests
import io
import numpy as np

from bblocks_data_importers.config import logger, Paths
from bblocks_data_importers.protocols import DataImporter


URL = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


def extract_data() -> io.BytesIO:
    """Extract GHED dataset"""

    try:
        return io.BytesIO(requests.get(URL).content)

    except ConnectionError:
        raise ConnectionError("Could not connect to WHO GHED database")


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
    """Importer for the GHED database from WHO"""

    def __init__(self):

        self._data_file_path = Paths.data / "ghed_data.feather"
        self._data = None

    def _load_data(self, check_disk = True) -> None:
        """Load the data to the object

        If the data exists in disk and check_disk is True, load it.
        Otherwise, download the data and save it to disk and load it to the object.

        Args:
            check_disk: If True, check if the data is in disk
        """

        if self._data_file_path.exists() and check_disk:
            logger.info("Loading data from disk")
            self._data = pd.read_feather(self._data_file_path)

        else:
            logger.info("Downloading data")
            self._data = get_data()
            self._data.to_feather(self._data_file_path)
            logger.info("Data saved to disk")

    def get_data(self, metadata=False) -> pd.DataFrame:
        """Get GHED data

        If the data has never been loaded, it will be downloaded and saved to disk.
        If the data has been loaded before, it will be loaded from disk.

        Args:
            metadata: If True, return the data including metadata
                        Default is False

        Returns:
            GHED data as a pandas DataFrame
        """

        if self._data is None:
            self._load_data()

        if not metadata:
            return self._data.loc[:, ['country_name', 'country_code',
                                      'indicator_code', 'indicator_name', 'year',
                                      'value']]

        return self._data

