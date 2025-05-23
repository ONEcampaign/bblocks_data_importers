"""Importer for World Bank Data

This importer provides functionality to easily get data from the World Bank databases.

It is a wrapper for the wbgapi package, which provides an easy-to-use interface to the World Bank API.

Usage:

First instantiate an importer object:
>>> wb = WorldBank()
"""

from typing import Iterable, Optional, Literal

import pandas as pd
import wbgapi
from wbgapi import Featureset

from bblocks.data_importers.config import Fields, logger
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.utilities import convert_dtypes


class WorldBank:
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

        search_indicators(query: str) -> dict:
            Searches for indicators by name and returns a dictionary with the indicator IDs and names.

        get_countries_by_income_level() -> dict:
            Returns a dictionary of countries by income level.

        get_economies_metadata(id: str | list = "all", labels: bool = True, skip_aggregates: bool = False) -> pd.DataFrame:
            Returns metadata for the specified economies.

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
            "most_recent_value": None,
            "api_params": {},
        }

    def __repr__(self) -> str:
        # Have we loaded any data yet?
        if self._data is not None:
            # pull out the unique series codes in the loaded DataFrame
            loaded_series = sorted(self._data[Fields.indicator_code].unique().tolist())
        else:
            loaded_series = []

        return (
            f"{self.__class__.__name__}("
            f"database={self.config['database']!r}, "
            f"economies={self.config['economies']!r}, "
            f"years={self.config['years']!r}, "
            f"loaded_series={loaded_series!r}, "
            f")"
        )

    def set_database(self, database: int) -> None:
        """Set the World Bank database to fetch data from.

        Args:
            database (int): The World Bank database to fetch data from.

        """
        self.config["database"] = database
        self.api.db = database

    def set_economies(self, economies: str | list[str]) -> None:
        """Set the economies to fetch data for.

        Args:
            economies (str | list[str]): The economies to fetch data for.

        """
        if isinstance(economies, str) and economies.lower() != "all":
            economies = [economies]

        self.config["economies"] = economies

    def set_years(self, years: str | int | list[int] | Iterable) -> None:
        """Set the years to fetch data for.

        Args:
            years (str | int | list[int] | Iterable): The years to fetch data for.

        """
        if isinstance(years, int):
            years = [years]

        self.config["years"] = years

    def set_most_recent_non_empty_value(self, values: int) -> None:
        """Set the number of most recent non-empty values to fetch.

        Args:
            value (int): How many most recent non-empty values to fetch.

        """
        self.config["most_recent_non_empty_value"] = values

    def set_most_recent_values_to_get(self, values: int) -> None:
        """Set whether to fetch the most recent value.

        Args:
            value (int): How many most recent values to fetch.

        """

        self.config["most_recent_value"] = values

    def set_api_params(self, params: dict) -> None:
        """Set additional parameters for the API request.

        Args:
            params (dict): Additional parameters for the API request.

        """
        self.config["api_params"] = params

    def search_indicators(self, query: str) -> dict:
        """Search for indicators by name. It looks in the currently selected database.

        Args:
            query (str): The query to search for.

        Returns:
            dict: A dictionary with the indicator IDs as keys and their names as values.
        """

        return self.api.series.Series(q=f"!{query}").to_dict()

    def get_available_databases(self, as_dict: bool = True) -> dict | Featureset:
        """Get a dictionary of available World Bank databases.

        Args:
            as_dict (bool): Whether to return the data as a dictionary or a Featureset object.

        Returns:
            dict | Featureset: A dictionary or Featureset object with the available databases.
            Featureset objects show up nicely on the console and contain additional information.
        """
        return self.api.source.Series().to_dict() if as_dict else self.api.source.info()

    def get_countries_by_income_level(self) -> dict:
        """Get a dictionary of countries by income level."""

        income_levels = self.api.income.Series().to_dict()

        return {
            value: self.api.income.members(id=key)
            for key, value in income_levels.items()
        }

    def get_countries_by_lending_type(self) -> dict:
        """Get a dictionary of countries by lending type."""

        lending_types = self.api.lending.Series().to_dict()

        return {
            value: self.api.lending.members(id=key)
            for key, value in lending_types.items()
        }

    def get_hipc_countries(self) -> list[str]:
        """Get a list of Heavily Indebted Poor Countries (HIPC)."""
        return list(self.api.region.members(id="HPC"))

    def get_ldc_countries(self) -> list[str]:
        """Get a list of Least Developed Countries (LDC)."""
        return list(self.api.region.members(id="LDC"))

    def get_countries_by_region(self) -> dict:
        """Get a dictionary of countries by region.

        There are over 40 regions in the World Bank API. This method may take
        a while to run if all regions are fetched.
        """

        logger.info(
            "Fetching countries by region. There are over 40 regions, so this may take a while."
        )

        regions = self.api.region.Series().to_dict()

        return {
            value: self.api.region.members(id=key) for key, value in regions.items()
        }

    def get_african_countries(self) -> list[str]:
        """Get a list of African economies."""
        return list(self.api.region.members(id="AFR"))

    def get_sub_saharan_african_countries(self) -> list[str]:
        """Get a list of Sub-Saharan African economies."""
        return list(self.api.region.members(id="SSF"))

    def get_sub_saharan_african_countries_excluding_high_income(self) -> list[str]:
        """Get a list of Sub-Saharan African economies excluding high-income countries."""
        return list(self.api.region.members(id="SSA"))

    def get_wb_fragile_and_conflict_affected_countries(self) -> list[str]:
        """Get a list of fragile and conflict-affected economies."""
        return list(self.api.region.members(id="FCS"))

    def get_economies_metadata(
        self, id: str | list = "all", labels: bool = True, skip_aggregates: bool = False
    ) -> pd.DataFrame:
        """Get metadata for the specified economies.

        Args:
            id (str | list): The ID(s) of the economies to fetch metadata for. Default is 'all'.
            labels (bool): Whether produce the dataframe with codes or labels.
            skip_aggregates (bool): Whether to skip groupings of countries.

        Returns:
            pd.DataFrame: A DataFrame with the metadata for the specified economies.

        """
        return self.api.economy.DataFrame(
            id=id, labels=labels, skipAggs=skip_aggregates
        )

    def get_indicator_metadata(self, indicator: str):
        """Get metadata for the specified indicator.

        Args:
            indicator (str): The indicator code to fetch metadata for.

        Returns:
            dict: A dictionary with the metadata for the specified indicator.
        """
        return self.api.series.metadata.get(id=indicator).metadata

    def wb_codes_to_names_mapping(self) -> dict:
        """Get a mapping of World Bank economy codes to names."""
        return self.get_economies_metadata(labels=False)["name"].to_dict()

    def add_entity_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add entity names to the data.

        Args:
            data (pd.DataFrame): The data to add entity names to.

        Returns:
            pd.DataFrame: The data with entity names added.
        """
        # insert entity names right after entity codes
        mapping = self.wb_codes_to_names_mapping()

        # Find the index of entity_code
        idx = data.columns.get_loc(Fields.entity_code)

        # Insert entity_name column
        data.insert(idx + 1, Fields.entity_name, data[Fields.entity_code].map(mapping))

        return data

    def _clean_data(self) -> None:
        """Clean the raw data by renaming columns, melting, and enforcing types."""

        # Drop duplicate time column
        data = self._raw_data["data"].drop(columns=["index"], errors="ignore")

        # rename columns
        data = (
            data.rename(
                columns={
                    "time": Fields.year,
                    "economy": Fields.entity_code,
                    "counterpart_area": Fields.counterpart_code,
                    "series": Fields.indicator_code,
                },
            )
            .set_index([Fields.year, Fields.entity_code, Fields.indicator_code])
            .reset_index()
        )

        # Add entity names
        data = self.add_entity_names(data)

        idx = (
            Fields.get_ids_idx()
            if self.config["database"] == 6
            else Fields.get_base_idx()
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
        self._raw_data["data"] = wbgapi.data.FlatFrame(
            series=series,
            db=self.config["database"],
            economy=self.config["economies"],
            time=self.config["years"],
            mrnev=self.config["most_recent_non_empty_value"],
            mrv=self.config["most_recent_value"],
            params=self.config["api_params"],
            skipBlanks=True,
            labels=False,
        ).reset_index()

        logger.info("Data successfully fetched from World Bank API")

    def clear_cache(self) -> None:
        """Clear the data cached in the importer"""

        self._raw_data = {}
        self._data = None

        logger.info("Cache cleared")

    def get_data(
        self,
        series: str | list[str],
        years: Optional[Literal["all"] | int | list[int] | Iterable] = None,
        economies: Optional[str | list[str] | Literal["all"]] = None,
        config: Optional[dict] = None,
    ) -> pd.DataFrame:
        """Fetches and returns data for the specified indicator series.

        Args:
            series (str | list[str]): The indicator code(s) to retrieve.
            Can be a single string or a list of strings.
            years: Optional[Literal["all"] | int | list[int] | Iterable]: Optionally, set the years to
             fetch data for. Specifying years here will override the instance configuration.
             All years are fetched by default
            economies (Optional[str | list[str] | Literal["all"]]): Optionally, set the economies to
                fetch data for. Specifying economies here will override the instance configuration.
                All economies are fetched by default.
            config (Optional[dict]): Optional configuration settings for fetching data,
            such as API parameters.

        Returns:
            pd.DataFrame: A DataFrame filtered to include only the requested indicator series.
        """
        # Ensure series is a list
        series = [series] if isinstance(series, str) else series

        if config is None:
            config = {}

        if isinstance(economies, str) and economies.lower() != "all":
            economies = [economies]

        # Set config overrides
        if years is not None:
            config["years"] = years
        if economies is not None:
            config["economies"] = economies

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
