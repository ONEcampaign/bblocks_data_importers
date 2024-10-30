"""Module to get data from WFP"""


import requests
import io
import pandas as pd
from typing import Literal
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
Paths
)
from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes

# food inflation: 71
# inflation YoY: 116
# inflation MoM: 117

HUNGERMAP_API = "https://api.hungermapdata.org/v2"
HUNGERMAP_HEADERS = {"referrer": "https://hungermap.wfp.org/",}
VAM_HEADERS = { "referrer": "https://dataviz.vam.wfp.org/"}


with open(Paths.project / "bblocks_data_importers/wfp/countries.json", 'r') as file:
    WFP_COUNTRY_DICT = json.load(file)


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
            for i in response.json()["body"]["features"] if "," not in i["properties"]["iso3"] and i["properties"]["dataType"] is not None
            }



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







def get_insufficient_food_subnational(country_id: int) -> dict:
    """ 249"""

    endpoint = f"https://api.hungermapdata.org/v2/adm0/{country_id}/adm1data.json"

    try:
        response = requests.get(endpoint, headers=HUNGERMAP_HEADERS)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        raise DataExtractionError(f"Error getting subnational insufficient food data for country {country_id}: {e}")








class WFPHunger(DataImporter):
    """Class to import data from the WFP Hunger Map API"""

    def __init__(self):

        self._countries: None | dict = None
        self._data_national = {}
        self._data_subnational = {}

    def load_available_countries(self, timeout: int = 20, retries: int = 1) -> None:
        """Load available countries to the object with timeout and retry mechanism"""

        endpoint = f"{HUNGERMAP_API}/adm0data.json"
        attempt = 0

        while attempt <= retries:
            try:
                logger.info("Getting country IDs")
                response = requests.get(endpoint, headers=HUNGERMAP_HEADERS, timeout=timeout)
                response.raise_for_status()

                # parse the response and create a dictionary
                self._countries = {
                    i["properties"]["iso3"]: {
                        "adm0_code": i["properties"]["adm0_id"],
                        "data_type": i["properties"]["dataType"]
                    }
                    for i in response.json()["body"]["features"]
                    if i["properties"]["dataType"] is not None
                }
                return  # Exit method if successful

            except requests.exceptions.Timeout:
                if attempt < retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out while getting country IDs after {retries + 1} attempts")

            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error getting country IDs after {retries + 1} attempts: {e}")
    @staticmethod
    def extract_data(adm0_code, level: Literal["national", "subnational"], timeout: int = 10, retries: int = 2) -> dict:
        """Extract the data from the source"""

        if level == "national":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/countryData.json"
        elif level == "subnational":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/adm1data.json"
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        attempt = 0
        while attempt <= retries:
            try:
                response = requests.get(endpoint, headers=HUNGERMAP_HEADERS, timeout=timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                if attempt < retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out for country adm0_code: {adm0_code} after {retries + 1} attempts")

            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error extracting data for country adm0_code: {adm0_code} after {retries + 1} attempts: {e}")

    def load_country_data(self, iso_code) -> None:
        """Load data to the object"""

        if not self._countries:
            self.load_available_countries()

        if iso_code not in self._countries:
            logger.info(f"No data found for country: {iso_code}")
            return

        response = self.extract_data(self._countries[iso_code]["adm0_code"], level = "national")

        # check that fcsGraph is in the response.
        # If the response is a list then no data was found or if fcsGraph is not in the response then no data was found
        if isinstance(response, list) or "fcsGraph" not in response:
            logger.info(f"No data found for country: {iso_code}")
            return

        df = (pd.DataFrame(response["fcsGraph"])
              .rename(columns = {"x": 'date', 'fcs': 'value', "fcsHigh": "value_upper", "fcsLow": "value_lower"})
              .assign(iso3_code = iso_code)
              .assign(data_type = self._countries[iso_code]["data_type"])
              )

        self._data_national[iso_code] = df


    def get_data(self, country_iso3_codes: str | list[str] | None = None,*, max_workers: int = 10) -> pd.DataFrame:
        """Get the data"""

        if self._countries is None:
            self.load_available_countries()

        if not country_iso3_codes:
            country_iso3_codes = self._countries.keys()
        elif isinstance(country_iso3_codes, str):
            country_iso3_codes = [country_iso3_codes]

        # Use ThreadPoolExecutor to load data concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks to load country data concurrently
            future_to_code = {
                executor.submit(self.load_country_data, code): code
                for code in country_iso3_codes
                if code not in self._data_national
            }

            # Process the completed tasks
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    future.result()  # Raises exception if load_country_data failed
                except Exception as e:
                    logger.error(f"Failed to load data for country {code}: {e}")

        return pd.concat([self._data_national[code] for code in country_iso3_codes if code in self._data_national],
                         ignore_index=True)





