"""Module to get data from WFP"""


import requests
import io
import pandas as pd

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
)

# food inflation: 71
# inflation YoY: 116
# inflation MoM: 117

HUNGERMAP_API = "https://api.hungermapdata.org/v2"
HUNGERMAP_HEADERS = {"referrer": "https://hungermap.wfp.org/",}
VAM_HEADERS = { "referrer": "https://dataviz.vam.wfp.org/"}


def get_wfp_country_ids() -> dict:
    """Get ADM0 country ID and iso3 code as a dictionary

    This function only returns countries with valid iso3 codes and excludes disputed and other
    territories that may still be tracked by the WFP.
    """

    endpoint = f"{HUNGERMAP_API}/adm0data.json"

    try:
        logger.info("Getting country IDs")
        response = requests.get(endpoint, headers=HUNGERMAP_HEADERS)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting country IDs: {e}")

    # parse the response and create a dictionary
    return {i["properties"]["iso3"]: i["properties"]["adm0_id"]
            for i in response.json()["body"]["features"] if "," not in i["properties"]["iso3"]}


def get_inflation_data(country_id: int, indicator_code: int | list[int],
                       start_date: str = None, end_date: str = None) -> io.BytesIO:
    """ """

    if isinstance(indicator_code, int):
        indicator_code = [indicator_code]

    url = "https://api.vam.wfp.org/economicExplorer/TradingEconomics/InflationExport"
    headers = {
        "referer": "https://dataviz.vam.wfp.org/"
    }
    data = {
        "adm0Code": country_id,
        "economicIndicatorIds": indicator_code,
    }

    if start_date:
        data["startDate"] = start_date
    if end_date:
        data["endDate"] = end_date

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return io.BytesIO(response.content)

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting inflation data: {e}")


def get_insufficient_food_national(country_id: int) -> dict:
    """ 248"""

    endpoint = f"https://api.hungermapdata.org/v2/adm0/{country_id}/countryData.json"

    try:
        response = requests.get(endpoint, headers=HUNGERMAP_HEADERS)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting national insufficient food data for country {country_id}: {e}")

def get_insufficient_food_subnational(country_id: int) -> dict:
    """ 249"""

    endpoint = f"https://api.hungermapdata.org/v2/adm0/{country_id}/adm1data.json"

    try:
        response = requests.get(endpoint, headers=HUNGERMAP_HEADERS)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting subnational insufficient food data for country {country_id}: {e}")






