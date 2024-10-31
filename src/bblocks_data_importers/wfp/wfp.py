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





class WFPHunger(DataImporter):
    """Class to import data from the WFP Hunger Map API"""

    def __init__(self,*, timeout: int = 20, retries: int = 2):

        self._timeout = timeout
        self._retries = retries

        self._countries: None | dict = None
        self._data_national = {}
        self._data_subnational = {}

    def _load_available_countries(self) -> None:
        """Load available countries to the object with timeout and retry mechanism"""

        endpoint = f"{HUNGERMAP_API}/adm0data.json"
        attempt = 0
        logger.info("Importing available country IDs")

        while attempt <= self._retries:
            try:
                response = requests.get(endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout)
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

                # exit loop if successful
                break

            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out while getting country IDs after {self._retries + 1} attempts")

            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error getting country IDs after {self._retries + 1} attempts: {e}")

    def _extract_data(self, adm0_code, level: Literal["national", "subnational"]) -> dict:
        """Extract the data from the source"""

        if level == "national":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/countryData.json"
        elif level == "subnational":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/adm1data.json"
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        attempt = 0
        while attempt <= self._retries:
            try:
                response = requests.get(endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out for adm0 code: {adm0_code} after {self._retries + 1} attempts")

            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error extracting data for country adm0_code: {adm0_code} after {self._retries + 1} attempts: {e}")

    @staticmethod
    def _parse_national_data(data: dict, iso_code: str) -> pd.DataFrame | None:
        """ """

        return (pd.DataFrame(data["fcsGraph"])
              .rename(columns = {"x": 'date', 'fcs': 'value', "fcsHigh": "value_upper", "fcsLow": "value_lower"})
              .assign(iso3_code = iso_code)
                .assign(indicator = "people with insufficient food consumption")
              # .assign(data_type = self._countries[iso_code]["data_type"])
              )

    @staticmethod
    def _parse_subnational_data(data: dict, iso_code: str) -> pd.DataFrame | None:
        """ """

        return (pd.concat([pd.DataFrame(_d['properties']['fcsGraph']).assign(region_name = _d['properties']['Name'])
                         for _d in data['features']
                         ], ignore_index = True)
              .rename(columns = {"x": "date", "fcs": "value", "fcsHigh": "value_upper", "fcsLow": "value_lower"})
              .assign(iso3_code = iso_code)
                .assign(indicator = "people with insufficient food consumption")
              )

    def _load_data(self, iso_code, level: Literal["national", "subnational"]) -> None:
        """Load data to the object"""

        # if a requested country is not available log no data found and return
        if iso_code not in self._available_countries_dict:
            logger.info(f"No data found for country: {iso_code}")
            return

        logger.info(f"Importing {level} data for country: {iso_code}")

        # extract, parse and load the data
        response = self._extract_data(self._available_countries_dict[iso_code]["adm0_code"], level = level)

        if level == "national":
            df = self._parse_national_data(response, iso_code)
            if df is not None:
                self._data_national[iso_code] = df

        elif level == "subnational":
            df = self._parse_subnational_data(response, iso_code)
            if df is not None:
                self._data_subnational[iso_code] = df

    @property
    def _available_countries_dict(self) -> dict:
        """Returns a dictionary of available countries with keys as country ISO3 codes
        """

        if self._countries is None:
            self._load_available_countries()

        return self._countries


    def get_data(self, country_iso3_codes: str | list[str] | None = None, level: Literal["national", "subnational"] = "national") -> pd.DataFrame:
        """Get the data"""

        # if no country is specified, get data for all available countries
        if not country_iso3_codes:
            country_iso3_codes = list(self._available_countries_dict.keys())

        # if a single country is specified, convert it to a list
        if isinstance(country_iso3_codes, str):
            country_iso3_codes = [country_iso3_codes]

        # load the data for the requested countries and level if not already loaded
        for code in country_iso3_codes:
            # if the data is already loaded, return
            if code not in self._data_national and level == "national":
                self._load_data(code, level)
            if code not in self._data_subnational and level == "subnational":
                self._load_data(code, level)


        # concatenate the dataframes
        if level == "national":
            df = pd.concat([self._data_national[code] for code in country_iso3_codes if code in self._data_national],
                           ignore_index=True)
        elif level == "subnational":
            df = pd.concat([self._data_subnational[code] for code in country_iso3_codes if code in self._data_subnational],
                           ignore_index=True)
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        # log if no data found
        if df.empty:
            logger.info("No data found for the requested countries")

        return df

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._data_national = {}
        self._data_subnational = {}
        self._countries = None
        logger.info("Cache cleared")

