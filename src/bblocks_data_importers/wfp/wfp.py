"""Module to get data from WFP"""


import requests
import io

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
)

# food inflation: 71
# inflation YoY: 116
# inflation MoM: 117



def get_country_ids() -> list[dict]:
    """Get country ID, name, and iso3 code from WFP"""

    url = "https://api.vam.wfp.org/dataviz/api/v1/ProxyEndpoints/GetExternalAPIResult"
    params = {
        "urlProxy": "https://api.vam.wfp.org/ReportsExplorer/v1/Countries",
    }
    headers = {
        "referrer": "https://dataviz.vam.wfp.org/",
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting country IDs: {e}")


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

    url = f"https://api.hungermapdata.org/v2/adm0/{country_id}/countryData.json"
    headers = {"referrer": "https://hungermap.wfp.org/",}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting national insufficient food data for country {country_id}: {e}")

def get_insufficient_food_subnational(country_id: int) -> dict:
    """ 249"""

    url = "https://api.hungermapdata.org/v2/adm0/248/adm1data.json"
    headers = {"referrer": "https://hungermap.wfp.org/",}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting subnational insufficient food data for country {country_id}: {e}")


