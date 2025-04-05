"""Human Development Index (HDI) data importer."""

import pandas as pd
import requests
import io
import numpy as np

from bblocks_data_importers.config import logger, DataExtractionError, Fields
from bblocks_data_importers.protocols import DataImporter


DATA_URL = "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv"
METADATA_URL = "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_metadata.xlsx"
DATA_ENCODING = "latin1" # Encoding used by the HDI data

def _request_hdi_data(url, *, timeout: int) -> requests.Response:
    """ Request the HDI data from the URL.

    Args:
        url (str): URL to request the HDI data from.
        timeout (int): Timeout for the request in seconds

    Returns:
        Response object containing the HDI data.
    """

    logger.debug("Requesting HDI data")

    try:

        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response

    except requests.RequestException as e:
        raise DataExtractionError(f"Error requesting HDI data: {e}") from e


def read_hdi_data(*, encoding = DATA_ENCODING, timeout: int = 30) -> pd.DataFrame:
    """ Read the HDI data from the response."""

    logger.debug("Reading HDI data")

    try:
        response = _request_hdi_data(DATA_URL, timeout=timeout)
        data = pd.read_csv(io.BytesIO(response.content), encoding=encoding)
        return data

    except pd.errors.ParserError as e:
        raise DataExtractionError(f"Error reading HDI data: {e}") from e


def read_hdi_metadata(*, timeout: int=30) -> pd.DataFrame:
    """ Read the HDI metadata from the response."""

    logger.debug("Reading HDI metadata")

    try:
        response = _request_hdi_data(METADATA_URL, timeout=timeout)
        metadata = pd.read_excel(io.BytesIO(response.content))
        return metadata

    except pd.errors.ParserError as e:
        raise DataExtractionError(f"Error reading HDI metadata: {e}") from e


def clean_metadata(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """ Clean the HDI metadata DataFrame.

    Args:
        metadata_df (pd.DataFrame): The HDI metadata DataFrame.

    Returns:
        The cleaned HDI metadata DataFrame.
    """

    return (metadata_df
     .dropna(subset="Time series")
     .rename(columns = {'Full name': Fields.indicator_name,
                        "Short name": Fields.indicator_code,
                        "Time series": Fields.time_range,
                        "Note": Fields.notes
                        })
     )



def clean_data(data_df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """ Clean the HDI data DataFrame.

    Args:
        data_df (pd.DataFrame): The HDI data DataFrame.
        metadata_df (pd.DataFrame): The HDI metadata DataFrame.

    Returns:
        The cleaned HDI data DataFrame.
    """

    return (data_df
            .rename(columns = {'iso3': Fields.entity_code, "country": Fields.entity_name, "region": Fields.region_code})
            .melt(id_vars = [Fields.entity_code, Fields.entity_name, Fields.region_code], var_name = Fields.indicator_code, value_name = Fields.value)
            .assign(split=lambda d: d.indicator_code.apply(lambda x: x.rsplit('_', 1) if '_' in x else [x, np.nan]))
            .assign(indicator_code=lambda x: x['split'].str[0], year=lambda x: x['split'].str[1])
            .drop(columns=['split'])
            .assign(indicator_name= lambda d: d.indicator_code.map(metadata_df.set_index("indicator_code")['indicator_name'].to_dict()))
            )


class HumanDevelopmentIndex(DataImporter):
    """A class to import Human Development Index (HDI) data from UNDP."""

    def __init__(self, *, timeout: int = 30):
        self._timeout = timeout
        self._data_df: pd.DataFrame | None = None
        self._metadata_df: pd.DataFrame | None = None


    def _extract_metadata(self):
        """Extract HDI metadata from the source."""

        logger.info("Extracting HDI metadata")

        metadata_df = read_hdi_metadata(timeout=self._timeout)
        self._metadata_df = clean_metadata(metadata_df)

    def _extract_data(self) -> None:
        """Extract HDI data from the source."""

        logger.info("Extracting HDI data")

        df = read_hdi_data(timeout=self._timeout)
        if self._metadata_df is None:
            self._extract_metadata()

        self._data_df = clean_data(df, self._metadata_df)


    def get_metadata(self) -> pd.DataFrame:
        """Get the HDI metadata

        Returns:
            The HDI metadata DataFrame.
        """

        if self._metadata_df is None:
            self._extract_metadata()
        return self._metadata_df

    def get_data(self) -> pd.DataFrame:
        """Get the HDI data

        Returns:
            The HDI data DataFrame.
        """

        if self._data_df is None:
            self._extract_data()
        return self._data_df

    def clear_cache(self) -> None:
        """Clear the cached data and metadata."""

        self._data_df = None
        self._metadata_df = None

        logger.info("Cache cleared")

