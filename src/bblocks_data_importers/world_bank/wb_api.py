"""Importer for World Bank Data

This importer provides functionality to easily get data from the World Bank databases.

It is a wrapper for the wbgapi package, which provides an easy-to-use interface to the World Bank API.

Usage:

First instantiate an importer object:
>>> wb = WorldBank()
"""

from typing import Iterable, Optional

import pandas as pd
import wbgapi

from bblocks_data_importers.protocols import DataImporter
from config import Fields, logger
from data_validators import DataFrameValidator
from utilities import convert_dtypes


class WorldBank(DataImporter):
    """World Bank Data Importer.

    This class provides a simplified interface for fetching and managing data from the World Bank databases.
    It leverages the `wbgapi` package to interact with the World Bank API, making it easy to retrieve
    and clean development indicators for analysis.

    The class supports configurable options for economies, years, and databases, and allows fine-tuning
    of API parameters. Users can set configurations such as which economies or years to fetch data for,
    whether to retrieve the most recent data, and more.

    Usage:
        1. Instantiate the importer object:
        >>> wb = WorldBank()

        2. Set any configurations if needed (e.g., setting the database or economies):
        >>> wb.set_database(2)
        >>> wb.set_economies("USA")

        3. Fetch data for specific indicator series:
        >>> data = wb.get_data(series="NY.GDP.MKTP.CD")

    Attributes:
        api (wbgapi): A reference to the World Bank API interface used to fetch data.
        config (dict): A dictionary holding the current configuration for data fetching.
            It includes settings such as 'economies', 'years', 'database', and 'api_params'.

    Methods:
        get_data(series: str | list[str], config: Optional[dict] = None) -> pd.DataFrame:
            Fetches data for the specified indicator series and returns a cleaned DataFrame.

        set_database(database: int) -> None:
            Sets the World Bank database to fetch data from.

        set_economies(economies: str | list[str]) -> None:
            Specifies the economies (countries) to fetch data for.

        set_years(years: str | int | list[int] | Iterable) -> None:
            Specifies the years to fetch data for.

        set_most_recent_non_empty_value(value: bool) -> None:
            Sets whether to fetch the most recent non-empty value.

        set_most_recent_value(value: bool) -> None:
            Sets whether to fetch the most recent value.

        set_api_params(params: dict) -> None:
            Sets additional parameters for the World Bank API request.

        clear_cache() -> None:
            Clears the cache of loaded data and configurations.
    """

    def __init__(self):
        self._raw_data: dict = {}
        self._data: pd.DataFrame | None = None

        # create a wbgapi object
        self.api = wbgapi

        # Set the valid configuration keys
        self._valid_config_keys = [
            "economies",
            "years",
            "database",
            "most_recent_non_empty_value",
            "most_recent_value",
            "api_params",
        ]

        # Set default configurations
        self.config = {
            "economies": "all",
            "years": "all",
            "database": 2,
            "most_recent_non_empty_value": False,
            "most_recent_value": False,
            "api_params": {},
        }

    def set_database(self, database: int) -> None:
        """Set the World Bank database to fetch data from.

        Args:
            database (int): The World Bank database to fetch data from.

        """
        self.config["database"] = database

    def set_economies(self, economies: str | list[str]) -> None:
        """Set the economies to fetch data for.

        Args:
            economies (str | list[str]): The economies to fetch data for.

        """
        self.config["economies"] = economies

    def set_years(self, years: str | int | list[int] | Iterable) -> None:
        """Set the years to fetch data for.

        Args:
            years (str | int | list[int] | Iterable): The years to fetch data for.

        """
        self.config["years"] = years

    def set_most_recent_non_empty_value(self, value: bool) -> None:
        """Set whether to fetch the most recent non-empty value.

        Args:
            value (bool): Whether to fetch the most recent non-empty value.

        """
        self.config["most_recent_non_empty_value"] = value

    def set_most_recent_value(self, value: bool) -> None:
        """Set whether to fetch the most recent value.

        Args:
            value (bool): Whether to fetch the most recent value.

        """
        self.config["most_recent_value"] = value

    def set_api_params(self, params: dict) -> None:
        """Set additional parameters for the API request.

        Args:
            params (dict): Additional parameters for the API request.

        """
        self.config["api_params"] = params

    def _clean_data(self) -> None:
        """Clean the raw data by renaming columns, melting, and enforcing types."""

        # Drop duplicate time column
        data = self._raw_data["data"].drop(columns=["Time"], errors="ignore")

        # rename columns
        data.rename(
            columns={
                "time": Fields.year,
                "economy": Fields.entity_code,
                "Country": Fields.country_name,
            },
            inplace=True,
        )

        # melt the DataFrame
        idx = [Fields.year, Fields.entity_code, Fields.country_name]
        data = data.melt(
            id_vars=idx, var_name=Fields.indicator_code, value_name=Fields.value
        )

        # Enforce types
        data = convert_dtypes(data)

        # validate
        DataFrameValidator().validate(
            data,
            required_cols=idx + [Fields.indicator_code, Fields.value],
        )

        # Load the cleaned data
        self._data = data

    def _load_wb_series(
        self,
        series: str | list[str],
        config: Optional[dict] = None,
    ) -> None:
        """Fetch a World Bank indicator and transform it into a cleaned DataFrame.

        Args:
            series str | list[str]: The World Bank indicator series code(s).
            config Optional[dict]: Configuration for the data fetch.

        Returns:
            None, the data is stored in the object.

        """
        logger.info(f"Fetching World Bank data for series: {series}")

        # Set the configuration
        if config is not None:
            self.config.update(config)

        # Store the configuration in the raw data
        self._raw_data["config"] = self.config

        # Fetch the indicator data from World Bank API, clean and structure it
        self._raw_data["data"] = wbgapi.data.DataFrame(
            series=series,
            db=self.config["database"],
            economy=self.config["economies"],
            time=self.config["years"],
            mrnev=self.config["most_recent_non_empty_value"],
            mrv=self.config["most_recent_value"],
            params=self.config["api_params"],
            columns="series",
            skipBlanks=True,
            numericTimeKeys=True,
            labels=True,
        ).reset_index()

        # If only one year is requested, the response will not include the year column
        # In this case, add the year to the DataFrame
        if len(self.config["years"]) == 1:
            self._raw_data["time"] = list(self.config["years"])[0]

        # If only one economy is requested, the response will not include the economy column
        # In this case, add the economy to the DataFrame
        if (
            isinstance(self.config["economies"], list)
            and len(self.config["economies"]) == 1
        ):
            self._raw_data["data"]["economy"] = self.config["economies"][0]
        elif isinstance(self.config["economies"], str):
            self._raw_data["economy"] = self.config["economies"]

        logger.info("Data successfully fetched from World Bank API")

    def clear_cache(self) -> None:
        """Clear the data cached in the importer"""

        self._raw_data = {}
        self._data = None

        logger.info("Cache cleared")

    def get_data(
        self, series: str | list[str], config: Optional[dict] = None
    ) -> pd.DataFrame:
        """Fetches and returns data for the specified indicator series.

        Args:
            series (str | list[str]): The indicator code(s) to retrieve.
            Can be a single string or a list of strings.
            config (Optional[dict]): Optional configuration settings for fetching data,
            such as API parameters.

        Returns:
            pd.DataFrame: A DataFrame filtered to include only the requested indicator series.
        """
        # Ensure series is a list
        series = [series] if isinstance(series, str) else series

        # Check that all configuration keys are valid
        if not set(config.keys()).issubset(self._valid_config_keys):
            raise ValueError(
                f"Invalid configuration keys: {set(config.keys()) - set(self._valid_config_keys)}"
            )

        # If the data is not loaded, load it
        if self._data is None:
            self._load_wb_series(series=series, config=config)
            self._clean_data()

        # If the data is loaded, check that the configuration is the same
        elif self._raw_data["config"].copy() | config != self._raw_data[
            "config"
        ] or not set(series).issubset(self._data[Fields.indicator_code].unique()):
            self.clear_cache()
            self._load_wb_series(series=series, config=config)
            self._clean_data()

        return self._data.loc[lambda d: d[Fields.indicator_code].isin(series)]
