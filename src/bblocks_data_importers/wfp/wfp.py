"""Module to get data from WFP"""
import io

import requests
import pandas as pd
import numpy as np
import country_converter as coco
from typing import Literal

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
    Units
)
from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import (
    convert_dtypes,
    convert_countries_to_unique_list,
)


HUNGERMAP_API: str = "https://api.hungermapdata.org/v2"
HUNGERMAP_HEADERS: dict = {"referrer": "https://hungermap.wfp.org/"}

VAM_API: str = "https://api.vam.wfp.org"
VAM_HEADERS: dict = {"referrer": "https://dataviz.vam.wfp.org/"}


INFLATION_IND_TYPE = Literal["Headline inflation (YoY)", "Headline inflation (MoM)", "Food inflation"]



def extract_countries(timeout: int = 20, retries: int = 2) -> dict:
    """Load available countries to the object with timeout and retry mechanism

    This method gets the countries tracked in HungerMap which have food security data available
    It collects their iso3 codes, adm0 codes and data types. Data types can be "ACTUAL", "PREDICTED" or "MIXED

    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s
        retries: The number of times to retry the request in case of a failure. Defaults to 2
    """

    endpoint = f"{HUNGERMAP_API}/adm0data.json"  # endpoint to get the country IDs

    # try to get the data from the API with retries
    attempt = 0
    while attempt <= retries:
        try:
            response = requests.get(
                endpoint, headers=HUNGERMAP_HEADERS, timeout=timeout
            )
            response.raise_for_status()

            # parse the response and create a dictionary
            return {
                i["properties"]["iso3"]: {
                    Fields.entity_code: i["properties"]["adm0_id"],
                    Fields.data_type: i["properties"]["dataType"],
                    Fields.country_name: coco.convert(
                        i["properties"]["iso3"], to="name_short", not_found=np.nan
                    ),
                }
                for i in response.json()["body"]["features"]
                if i["properties"]["dataType"] is not None
            }

        # handle timeout errors
        except requests.exceptions.Timeout:
            if attempt < retries:
                attempt += 1
            else:
                raise DataExtractionError(
                    f"Request timed out while getting country IDs after {retries + 1} attempts"
                )

        # handle other request errors
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                attempt += 1
            else:
                raise DataExtractionError(
                    f"Error getting country IDs after {retries + 1} attempts: {e}"
                )




class WFPInflation(DataImporter):
    """A class to import inflation data from the World Food Programme (WFP)

    The World Food Programme (WFP) provides data on inflation for various countries collected
    from Trading Economics. The data is available for different indicators including headline inflation (year-on-year
    and month-on-month) and food inflation.
    See the data at: https://dataviz.vam.wfp.org/economic/inflation

    Usage:
    First instantiate an importer object:
    >>> wfp = WFPInflation()

    Optionally set the timeout for the requests in seconds. By default, it is set to 20s.:
    >>> wfp = WFPInflation(timeout = 30)

    See the available indicators:
    >>> wfp.available_indicators

    Get the data:
    >>> data = wfp.get_data(indicator = "Headline inflation (YoY)", country = ["KEN", "UGA"])
    If no indicator is specified, data for all available indicators is returned and if no country is specified, data for all available countries is returned.
    It is advised to specify the required indicator and countries to avoid long wait times.

    To clear the cache:
    >>> wfp.clear_cache()


    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s

    """

    def __init__(self, *, timeout: int = 20):

        self._timeout = timeout
        self._indicators = {"Headline inflation (YoY)": 116,
                            "Headline inflation (MoM)": 117,
                            "Food inflation": 71
                            }
        self._countries = None # available countries
        self._data = {"Headline inflation (YoY)": {},
                     "Headline inflation (MoM)": {},
                     "Food inflation": {}
                     }

    def load_available_countries(self):
        """Load available countries to the object
        """

        logger.info("Importing available country IDs ...")
        self._countries = extract_countries(self._timeout)

    def extract_data(self, country_code: int, indicator_code: int | list[int]) -> io.BytesIO:
        """Extract the data from the source

        Queries the WFP API to get the inflation data for a specific country and indicator

        Args:
            country_code: The adm0 code of the country
            indicator_code: The indicator code. Can be a single code or a list of codes

        Returns:
            A BytesIO object with the data
        """

        if isinstance(indicator_code, int):
            indicator_code = [indicator_code]

        endpoint = f"{VAM_API}/economicExplorer/TradingEconomics/InflationExport"
        params = {
            "adm0Code": country_code,
            "economicIndicatorIds": indicator_code,
            # "endDate": "2024-10-31", # defaults to latest
            # "startDate": "2023-07-01", # defaults to all available
        }

        try:
            resp = requests.post(endpoint, json=params, headers=VAM_HEADERS, timeout=self._timeout)
            resp.raise_for_status()
            return io.BytesIO(resp.content)

        except requests.exceptions.Timeout:
            raise DataExtractionError("Request timed out while getting inflation data")

        except requests.exceptions.RequestException as e:
            raise DataExtractionError(f"Error getting inflation data: {e}")

    @staticmethod
    def format_data(data: io.BytesIO, indicator_name: str, iso3_code: str) -> pd.DataFrame:
        """Format the data

        This method reads the data from the BytesIO object, formats it and returns a DataFrame

        Args:
            data: The BytesIO object with the data
            indicator_name: The name of the indicator
            iso3_code: The ISO3 code of the country

        Returns:
            A DataFrame with the formatted data
        """

        return (pd.read_csv(data)
        .drop(columns = ["IndicatorName", "CountryName"]) # drop unnecessary columns
         .rename(columns = {'Date': Fields.date, "Value": Fields.value, 'SourceOfTheData': Fields.source})
                .pipe(convert_dtypes)
         .assign(**{
            Fields.indicator_name: indicator_name,
            Fields.iso3_code: iso3_code,
            Fields.country_name:coco.convert(iso3_code, to = 'name_short', not_found = np.nan),
            Fields.date: lambda d: pd.to_datetime(d[Fields.date], format="%d/%m/%Y"),
            Fields.unit: Units.percent
            })
         )

    def load_data(self, indicator_name: str, iso3_codes: list[str]) -> None:
        """Load data to the object

        This method runs the process to extract, format and load the data to the object for a specific indicator
        and list of countries. It checks if the data is already loaded for specified countries and skips the process if it is. If the data
        is not available for a specific country, it logs a warning and sets the data to None.

        Args:
            indicator_name: The name of the indicator
            iso3_codes: A list of ISO3 codes of the countries to load the data for
        """

        # make a list of unloaded countries
        unloaded_countries = [c for c in iso3_codes if c not in self._data[indicator_name]]

        # if all countries have been loaded skip the process
        if len(unloaded_countries) == 0:
            return None

        logger.info(f"Importing data for indicator: {indicator_name} ...")

        for iso3_code in unloaded_countries:

            # if the country is not available raise a warning set the data to None and continue
            if iso3_code not in self._countries:
                logger.warning(f"Data not found for country - {iso3_code}")
                self._data[indicator_name][iso3_code] = None
                continue

            # extract the data, format it and load it to the object
            data = self.extract_data(self._countries[iso3_code]['entity_code'], self._indicators[indicator_name])
            df = self.format_data(data, indicator_name, iso3_code)

            # if the dataframe is empty log a warning, set the data to None and continue
            if df.empty:
                logger.warning(f"No {indicator_name} data found for country - {iso3_code}")
                self._data[indicator_name][iso3_code] = None
                continue

            self._data[indicator_name][iso3_code] = df

        logger.info(f"Data imported successfully for indicator: {indicator_name}")

    @property
    def available_indicators(self) -> list[str]:
        """Returns a list of available indicators"""

        return list(self._indicators.keys())

    def get_data(self, indicator: INFLATION_IND_TYPE | list[INFLATION_IND_TYPE] | None = None, country: str | list[str] = None) -> pd.DataFrame:
        """Get inflation data

        Get a dataframe with the data for the specified inflation indicator and countries

        Args:
            indicator: The inflation indicator to get data for. Can be a single indicator or a list of indicators.
                If None, data for all available indicators is returned. By default, returns all available indicators.
                To see the available indicators use the available_indicators property
            country: The countries (name or ISO3 code) to get data for. If None, data for all available countries is returned
                By default returns data for all available countries

        Returns:
            A DataFrame with the requested data
        """

        if indicator:
            if isinstance(indicator, str):
                indicator = [indicator]

            # check that all indicators are valid
            for ind in indicator:
                if ind not in self._indicators:
                    raise ValueError(f"Invalid indicator - {ind}. Please choose from {list(self._indicators.keys())}")

        # if no indicator is specified, get data for all available indicators
        else:
            indicator = list(self._indicators.keys())


        # check if country IDs are loaded, if not then load them
        if self._countries is None:
            self.load_available_countries()

        # validate countries
        if country:
            if isinstance(country, str):
                country = [country]

            # check that countries are valid, if not then drop them to make a unique list
            country = convert_countries_to_unique_list(country, to="ISO3")

            # if the list is empty then raise an error
            if len(country) == 0:
                raise ValueError("No valid countries found")

        # if no country is specified, get data for all available countries
        else:
            country = list(self._countries.keys())


        # load the data for the requested countries and indicators if not already loaded
        for ind in indicator:
            self.load_data(indicator_name=ind, iso3_codes=country)

        # concatenate the dataframes for the requested countries and indicators if available
        data_list = [self._data[ind][code]
                     for ind in indicator for code in country
                     if code in self._data[ind] and self._data[ind][code] is not None]

        # if no data is found return an empty DataFrame and log a warning
        if len(data_list) == 0:
            logger.warning("No data found for the requested countries")
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index = True)

    def clear_cache(self) -> None:
        """Clear the cached data"""

        self._data = {"Headline inflation (YoY)": {},
                     "Headline inflation (MoM)": {},
                     "Food inflation": {}
                     }
        self._countries = None

        logger.info("Cache cleared")



















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

    To get the available countries and their details:
    >>> wfp.available_countries

    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s
        retries: The number of times to retry the request in case of a failure. Defaults to 2

    Indicator definition:
    People with insufficient food consumption refer to those with poor or borderline food consumption, according to the Food Consumption Score (FCS). The Food Consumption Score (FCS) is a proxy of household's food access and a core WFP indicator used to classify households into different groups based on the adequacy of the foods consumed in the week prior to being surveyed. FCS is the most commonly used food security indicator by WFP and partners. This indicator is a composite score based on households’ dietary diversity, food frequency, and relative nutritional importance of different food groups. The FCS is calculated using the frequency of consumption of eight food groups by a household during the 7 days before the survey using standardized weights for each of the food groups reflecting its respective nutrient density, and then classifies households as having ‘poor’, ‘borderline’ or ‘acceptable’ food consumption: Poor food consumption: Typically refers to households that are not consuming staples and vegetables every day and never or very seldom consume protein-rich food such as meat and dairy (FCS of less than 21 or 28). Borderline food consumption: Typically refers to households that are consuming staples and vegetables every day, accompanied by oil and pulses a few times a week (FCS of less than 35 or 42). Acceptable food consumption: Typically refers to households that are consuming staples and vegetables every day, frequently accompanied by oil and pulses, and occasionally meat, fish and dairy (FCS greater than 42).

    """

    def __init__(self, *, timeout: int = 20, retries: int = 2):

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

        endpoint = f"{HUNGERMAP_API}/adm0data.json"  # endpoint to get the country IDs

        # try to get the data from the API with retries
        attempt = 0
        while attempt <= self._retries:
            try:
                response = requests.get(
                    endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout
                )
                response.raise_for_status()

                # parse the response and create a dictionary
                self._countries = {
                    i["properties"]["iso3"]: {
                        Fields.entity_code: i["properties"]["adm0_id"],
                        Fields.data_type: i["properties"]["dataType"],
                        Fields.country_name: coco.convert(
                            i["properties"]["iso3"], to="name_short", not_found=np.nan
                        ),
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
                    raise DataExtractionError(
                        f"Request timed out while getting country IDs after {self._retries + 1} attempts"
                    )

            # handle other request errors
            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(
                        f"Error getting country IDs after {self._retries + 1} attempts: {e}"
                    )

    def _extract_data(
        self, entity_code: int, level: Literal["national", "subnational"]
    ) -> dict:
        """Extract the data from the source

        Args:
            entity_code: The adm0 code of the country
            level: The level of data to extract. Can be "national" or "subnational"

        Returns:
            the json response from the API
        """

        # get the specific endpoint based on the level
        if level == "national":
            endpoint = (
                f"https://api.hungermapdata.org/v2/adm0/{entity_code}/countryData.json"
            )
        elif level == "subnational":
            endpoint = (
                f"https://api.hungermapdata.org/v2/adm0/{entity_code}/adm1data.json"
            )
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        # try to get the data from the API with retries
        attempt = 0
        while attempt <= self._retries:
            try:
                response = requests.get(
                    endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout
                )
                response.raise_for_status()
                return response.json()

            # handle timeout errors
            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(
                        f"Request timed out for adm0 code - {entity_code} after {self._retries + 1} attempts"
                    )

            # handle other request errors
            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(
                        f"Error extracting data for country adm0_code - {entity_code} after {self._retries + 1} attempts: {e}"
                    )

    @staticmethod
    def _parse_national_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the national data

        This method parses the national data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart
        for the indicator "people with insufficient food consumption" over time

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        try:

            return (
                pd.DataFrame(data["fcsGraph"])
                .rename(
                    columns={
                        "x": Fields.date,
                        "fcs": Fields.value,
                        "fcsHigh": Fields.value_upper,
                        "fcsLow": Fields.value_lower,
                    }
                )
                .assign(
                    **{
                        Fields.iso3_code: iso_code,
                        Fields.country_name: coco.convert(
                            iso_code, to="name_short", not_found=np.nan
                        ),
                        Fields.indicator_name: "people with insufficient food consumption",
                        Fields.source: "World Food Programme",
                    }
                )
                .pipe(convert_dtypes)
                .assign(
                    **{Fields.date: lambda d: pd.to_datetime(d.date, format="%Y-%m-%d")}
                )
            )

        except Exception as e:
            raise DataFormattingError(
                f"Error parsing national data for country - {iso_code}: {e}"
            )

    @staticmethod
    def _parse_subnational_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the subnational data

        This method parses the subnational data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart for
        each region for the indicator "people with insufficient food consumption" over time. The method
        loops through the regions concatenating the data for each region into a single DataFrame

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        try:
            return (
                pd.concat(
                    [
                        pd.DataFrame(_d["properties"]["fcsGraph"]).assign(
                            region_name=_d["properties"]["Name"]
                        )
                        for _d in data["features"]
                    ],
                    ignore_index=True,
                )
                .rename(
                    columns={
                        "x": Fields.date,
                        "fcs": Fields.value,
                        "fcsHigh": Fields.value_upper,
                        "fcsLow": Fields.value_lower,
                    }
                )
                .assign(
                    **{
                        Fields.iso3_code: iso_code,
                        Fields.country_name: coco.convert(
                            iso_code, to="name_short", not_found=np.nan
                        ),
                        Fields.indicator_name: "people with insufficient food consumption",
                        Fields.source: "World Food Programme",
                    }
                )
                .pipe(convert_dtypes)
                .assign(
                    **{Fields.date: lambda d: pd.to_datetime(d.date, format="%Y-%m-%d")}
                )
            )

        except Exception as e:
            raise DataFormattingError(
                f"Error parsing subnational data for country - {iso_code}: {e}"
            )

    def _load_data(
        self, iso_code: str, level: Literal["national", "subnational"]
    ) -> None:
        """Load data to the object

        This method runs the process to extract, parse and load the data to the object for a specific country
        at the specified level

        Args:
            iso_code: The iso3 code of the country
            level: The level of data to load. Can be "national" or "subnational"
        """

        # if a requested country is not available log no data found and return
        if iso_code not in self._available_countries_dict:
            logger.info(f"No data found for country - {iso_code}")
            return None

        logger.info(f"Importing {level} data for country - {iso_code} ...")

        # extract, parse and load the data
        response = self._extract_data(
            self._available_countries_dict[iso_code][Fields.entity_code], level=level
        )

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

        return (
            pd.DataFrame(self._countries)
            .T.reset_index()
            .rename(columns={"index": Fields.iso3_code})
            .pipe(convert_dtypes)
        )

    def get_data(
        self,
        countries: str | list[str] | None = None,
        level: Literal["national", "subnational"] = "national",
    ) -> pd.DataFrame:
        """Get data for "people with insufficient food consumption"

        Args:
            countries: The countries (name or ISO3 code) to get data for. If None, data for all available countries is returned
            level: The level of data to get. Can be "national" or "subnational". Defaults to "national"

        Returns:
            A DataFrame with the data
        """

        # if no country is specified, get data for all available countries
        if not countries:
            countries = list(self._available_countries_dict.keys())

        else:
            # if a single country is specified, convert it to a list
            if isinstance(countries, str):
                countries = [countries]

            # convert the country names to ISO3 codes
            countries = convert_countries_to_unique_list(countries, to="ISO3")

        # load the data for the requested countries and level if not already loaded
        for code in countries:
            # if the data is already loaded, return
            if code not in self._data_national and level == "national":
                self._load_data(code, level)
            if code not in self._data_subnational and level == "subnational":
                self._load_data(code, level)

        # concatenate the dataframes
        if level == "national":
            data_list = [
                self._data_national[code]
                for code in countries
                if code in self._data_national
            ]
        elif level == "subnational":
            data_list = [
                self._data_subnational[code]
                for code in countries
                if code in self._data_subnational
            ]
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        if len(data_list) == 0:
            logger.warning("No data found for the requested countries")
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index=True)

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._data_national = {}
        self._data_subnational = {}
        self._countries = None
        logger.info("Cache cleared")
