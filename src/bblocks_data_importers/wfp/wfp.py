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


HUNGERMAP_API: str = "https://api.hungermapdata.org/v2"
HUNGERMAP_HEADERS: dict = {"referrer": "https://hungermap.wfp.org/"}


class WFPFoodSecurity(DataImporter):
    """Class to import food security data from the WFP Hunger Map API

    The World Food Programme (WFP) Hunger Map is a global hunger monitoring system which provides data
    on food security and other related indicators.

    The data accessible through this object is "people with insufficient food consumption" and is available at the national and subnational levels.
    Access the Hunger Map at: https://hungermap.wfp.org/

    Usage:
    First instantiate an importer object:
    >>> wfp = WFPFoodSecurity()

    Get the data:
    >>> data = wfp.get_data(country_iso3_codes = ["KEN", "UGA"])
    This will return a pandas DataFrame with the data for the specified countries.

    You can also get the data at the subnational level:
    >>> data = wfp.get_data(country_iso3_codes = ["KEN", "UGA"], level = "subnational")

    To clear the cache:
    >>> wfp.clear_cache()


    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s
        retries: The number of times to retry the request in case of a failure. Defaults to 2

    Indicator definition:
    People with insufficient food consumption refer to those with poor or borderline food consumption, according to the Food Consumption Score (FCS). The Food Consumption Score (FCS) is a proxy of household's food access and a core WFP indicator used to classify households into different groups based on the adequacy of the foods consumed in the week prior to being surveyed. FCS is the most commonly used food security indicator by WFP and partners. This indicator is a composite score based on households’ dietary diversity, food frequency, and relative nutritional importance of different food groups. The FCS is calculated using the frequency of consumption of eight food groups by a household during the 7 days before the survey using standardized weights for each of the food groups reflecting its respective nutrient density, and then classifies households as having ‘poor’, ‘borderline’ or ‘acceptable’ food consumption: Poor food consumption: Typically refers to households that are not consuming staples and vegetables every day and never or very seldom consume protein-rich food such as meat and dairy (FCS of less than 21 or 28). Borderline food consumption: Typically refers to households that are consuming staples and vegetables every day, accompanied by oil and pulses a few times a week (FCS of less than 35 or 42). Acceptable food consumption: Typically refers to households that are consuming staples and vegetables every day, frequently accompanied by oil and pulses, and occasionally meat, fish and dairy (FCS greater than 42).

    """

    def __init__(self,*, timeout: int = 20, retries: int = 2):

        self._timeout = timeout
        self._retries = retries

        self._countries: None | dict = None
        self._data_national: dict = {}
        self._data_subnational: dict = {}

    def _load_available_countries(self) -> None:
        """Load available countries to the object with timeout and retry mechanism

        This method gets the countries tracked in HungerMap which have food security data available
        It collects their iso3 codes, adm0 codes and data types. Data types can be "ACTUAL", "PREDICTED" or "MIXED
        """

        logger.info("Importing available country IDs ...")

        endpoint = f"{HUNGERMAP_API}/adm0data.json" # endpoint to get the country IDs

        # try to get the data from the API with retries
        attempt = 0
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

            # handle timeout errors
            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out while getting country IDs after {self._retries + 1} attempts")

            # handle other request errors
            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error getting country IDs after {self._retries + 1} attempts: {e}")

    def _extract_data(self, adm0_code: int, level: Literal["national", "subnational"]) -> dict:
        """Extract the data from the source

        Args:
            adm0_code: The adm0 code of the country
            level: The level of data to extract. Can be "national" or "subnational"

        Returns:
            the json response from the API
        """

        # get the specific endpoint based on the level
        if level == "national":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/countryData.json"
        elif level == "subnational":
            endpoint = f"https://api.hungermapdata.org/v2/adm0/{adm0_code}/adm1data.json"
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        # try to get the data from the API with retries
        attempt = 0
        while attempt <= self._retries:
            try:
                response = requests.get(endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout)
                response.raise_for_status()
                return response.json()

            # handle timeout errors
            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Request timed out for adm0 code: {adm0_code} after {self._retries + 1} attempts")

            # handle other request errors
            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(f"Error extracting data for country adm0_code: {adm0_code} after {self._retries + 1} attempts: {e}")

    @staticmethod
    def _parse_national_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the national data

        This method parses the national data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart
        for the indicator "people with insufficient food consumption" over time

        TODO: convert dtypes
        TODO: add source and country name

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        return (pd.DataFrame(data["fcsGraph"])
                .rename(columns = {"x": 'date', 'fcs': 'value', "fcsHigh": "value_upper", "fcsLow": "value_lower"})
                .assign(iso3_code = iso_code,
                        indicator = "people with insufficient food consumption",
                        )
                .pipe(convert_dtypes)
                .assign(date=lambda d: pd.to_datetime(d.date, format="%Y-%m-%d"))
              )

    @staticmethod
    def _parse_subnational_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the subnational data

        This method parses the subnational data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart for
        each region for the indicator "people with insufficient food consumption" over time. The method
        loops through the regions concatenating the data for each region into a single DataFrame

        TODO: convert dtypes
        TODO: add source and country name

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        return (pd.concat([pd.DataFrame(_d['properties']['fcsGraph']).assign(region_name = _d['properties']['Name'])
                         for _d in data['features']
                         ], ignore_index = True)
                .rename(columns = {"x": "date", "fcs": "value", "fcsHigh": "value_upper", "fcsLow": "value_lower"})
                .assign(iso3_code = iso_code,
                        indicator = "people with insufficient food consumption",
                        )
                .pipe(convert_dtypes)
                .assign(date=lambda d: pd.to_datetime(d.date, format="%Y-%m-%d"))
              )

    def _load_data(self, iso_code: str, level: Literal["national", "subnational"]) -> None:
        """Load data to the object

        This method runs the process to extract, parse and load the data to the object for a specific country
        at the specified level

        Args:
            iso_code: The iso3 code of the country
            level: The level of data to load. Can be "national" or "subnational"
        """

        # if a requested country is not available log no data found and return
        if iso_code not in self._available_countries_dict:
            logger.info(f"No data found for country: {iso_code}")
            return None

        logger.info(f"Importing {level} data for country: {iso_code} ...")

        # extract, parse and load the data
        response = self._extract_data(self._available_countries_dict[iso_code]["adm0_code"], level = level)

        # parse and load the data
        if level == "national":
            df = self._parse_national_data(response, iso_code)
            self._data_national[iso_code] = df
        if level == "subnational":
            df = self._parse_subnational_data(response, iso_code)
            self._data_subnational[iso_code] = df

    @property
    def _available_countries_dict(self) -> dict:
        """Returns a dictionary of available countries with keys as country ISO3 codes
        and values as dictionaries with the adm0 code and data type
        If the countries are not loaded, it loads them
        """

        if self._countries is None:
            self._load_available_countries()

        return self._countries

    @property
    def available_countries(self) -> pd.DataFrame:
        """Returns a DataFrame with the available countries and their details"""

        if self._countries is None:
            self._load_available_countries()

        return (pd.DataFrame(self._countries)
                .T
                .reset_index()
                .rename(columns = {"index": "iso3_code", "adm0_code": "country_id", "data_type": "data_type"})
                .pipe(convert_dtypes)
               )


    def get_data(self, country_iso3_codes: str | list[str] | None = None, level: Literal["national", "subnational"] = "national") -> pd.DataFrame:
        """Get data for "people with insufficient food consumption"

        Args:
            country_iso3_codes: The iso3 codes of the countries to get the data for. If None, data for all available countries is returned
            level: The level of data to get. Can be "national" or "subnational". Defaults to "national"

        Returns:
            A DataFrame with the data
        """

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
            data_list = [self._data_national[code] for code in country_iso3_codes if code in self._data_national]
        elif level == "subnational":
            data_list = [self._data_subnational[code] for code in country_iso3_codes if code in self._data_subnational]
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        if len(data_list) == 0:
            logger.info("No data found for the requested countries")
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index = True)

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._data_national = {}
        self._data_subnational = {}
        self._countries = None
        logger.info("Cache cleared")

