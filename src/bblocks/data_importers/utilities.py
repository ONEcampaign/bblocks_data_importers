import pandas as pd
from typing import Literal
import country_converter as coco

from bblocks.data_importers.config import logger


def convert_dtypes(
    df: pd.DataFrame, backend: Literal["pyarrow", "numpy_nullable"] = "pyarrow"
) -> pd.DataFrame:
    """Converts the DataFrame to the specified backend dtypes

    Args:
        df: The DataFrame to convert
        backend: The backend to use for the conversion. Default is "pyarrow"

    Returns:
        A DataFrame with the pyarrow dtypes
    """

    supported_backends = {"pyarrow", "numpy_nullable"}

    # Check if the backend is valid
    if backend not in supported_backends:
        raise ValueError(
            f"Unsupported backend '{backend}'. Supported backends are {supported_backends}."
        )

    # Convert dtypes using the specified backend
    return df.convert_dtypes(dtype_backend=backend)


def convert_countries_to_unique_list(
    countries: list, src: str | None = None, to: str = "ISO3"
) -> list:
    """Converts a list of country names to a unique list of countries in the specified format

    Args:
        countries: A list of country names
        src: The source format of the country names. Default is None, uses the conversion mechanism of the country_converter package to determine the source format
        to: The format to convert the country names to. Default is "ISO3"

    Returns:
        A unique list of countries in the specified format
    """

    converted_list = set()

    for country in countries:
        converted_country = coco.convert(country, src=src, to=to)
        if converted_country == "not found":
            logger.warning(f"Country not found: {country}")
        else:
            converted_list.add(converted_country)

    return list(converted_list)
