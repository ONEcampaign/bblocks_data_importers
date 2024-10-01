"""Importer for the GHED database from WHO"""


import pandas as pd
import requests
import io
import numpy as np


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

