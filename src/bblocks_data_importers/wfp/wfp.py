"""Module to get data from WFP"""


import requests

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
)


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