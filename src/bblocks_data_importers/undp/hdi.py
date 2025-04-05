"""Human Development Index (HDI) data importer."""

import pandas as pd
import requests
import io

from bblocks_data_importers.config import logger, DataExtractionError, Fields


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


