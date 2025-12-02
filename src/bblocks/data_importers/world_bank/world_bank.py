"""World Bank"""

from functools import lru_cache
from typing import Generator
import pandas as pd
import wbgapi as wb

from bblocks.data_importers.config import logger, Fields, DataExtractionError
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.data_validators import DataFrameValidator


def _make_hashable(value) -> object:
    """Convert unhashable types to hashable types for caching purposes."""
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, dict):
        return tuple(sorted(value.items()))
    return value


class WorldBank:
    """ """

    def __init__(self, db: int | None = None):

        self._databases: pd.DataFrame | None = None # available databases
        self._indicators: dict[int, pd.DataFrame] = {} # dictionary of indicators per database

        # self._economies: pd.DataFrame | None = None
        # self._regions: pd.DataFrame | None = None


        # Set the database if provided
        if db is not None:
            self.set_db(db)
        else:
            self._db = wb.db
            logger.info(f"World Bank database set to {self._db}.")


    def _clean_wb_series_list(self, series: pd.Series, columns: list[str]) -> pd.DataFrame:
        """Clean returned wbgapi series list eg databases, economies etc into a standard DataFrame

        Args:
            series: The pandas Series returned from wbgapi
            columns: The desired column names for the DataFrame
        Returns:
            A cleaned DataFrame
        """

        df = series.reset_index()
        df.columns = columns

        return df

    def get_available_databases(self) -> pd.DataFrame:
        """Get the available World Bank databases."""

        if self._databases is None:
            self._databases = (self._clean_wb_series_list(wb.source.Series(),
                                                        columns=["db_id", "db_name"]
                                                     )
                               # set db_id as integer
                                 .assign(db_id=lambda d: d.db_id.astype(int))
                               )
        return self._databases

    def _check_valid_db(self, db: int) -> None:
        """Check if the provided database id is valid."""

        if db not in self.get_available_databases()["db_id"].unique():
            raise ValueError(f"Database ID {db} is not valid.")



    @property
    def db(self) -> int:
        """Get the current World Bank database."""

        if self._db is None:
            raise AttributeError("The database has not been set yet. Use the `set_db` method to set the database"
                                 " or use the `get_available_databases` method to see the available databases."
                                 " By default, calling the `get_data` method without setting a database will query the"
                                 " World Development Indicators database (id=2)")
        return self._db

    def set_db(self, db: int) -> None:
        """Set the World Bank database to use.
        To see the available databases, use the `get_available_databases` method.

        Args:
            db: The database id.
        """

        self._check_valid_db(db)
        self._db = db
        logger.info(f"World Bank database set to {db}.")


    def get_available_indicators(self, db: int | None = None) -> pd.DataFrame:
        """Get available indicators for the current or specified database.

        Args:
            db: The database id. If None, uses the currently set database.

        Returns:
            A DataFrame with the available indicators for the specified database.
        """

        if db is None:
            db = self.db

        self._check_valid_db(db)

        if db not in self._indicators:
            wb.db = db # set the database in the wbgapi
            self._indicators[db] = self._clean_wb_series_list(wb.series.Series(),
                                                              columns=[Fields.indicator_code, Fields.indicator_name])

        return self._indicators[db]


    # def get_available_entities(self) -> pd.DataFrame:
    #     """Get available economies from the World Bank."""
    #
    #     if self._economies is None:
    #         self._economies = self._clean_wb_series_list(wb.economy.Series(),
    #                                                      columns=[Fields.entity_code, Fields.entity_name]
    #                                                   )
    #
    #     return self._economies

    # def get_available_regions(self) -> pd.DataFrame:
    #     """Get available regions from the World Bank."""
    #
    #     if self._regions is None:
    #         self._regions = self._clean_wb_series_list(wb.region.Series(),
    #                                                    columns=[Fields.region_code, Fields.region_name]
    #                                                 )
    #
    #     return self._regions

    @lru_cache(maxsize=8)
    def _fetch_data(self, indicator_code: str | tuple[str],
                    db: int | None,
                    entity_code: str | tuple[str] | None,
                    time: int | range,
                    skip_blanks: bool,
                    skip_aggs: bool,
                    include_labels: bool,
                    params_items: tuple | None,
                    extra_items: tuple) -> pd.DataFrame:
        """Fetch data from the World Bank API.

        Fetches data using wbgapi with caching to avoid redundant API calls. Cache size is limited to 8 unique queries.
        The returned generator from wbgapi is checked for data presence before processing. If no data is returned,
        a DataExtractionError is raised. The data is then flattened into a DataFrame.

        """

        params = dict(params_items) if params_items is not None else None
        extra = dict(extra_items)

        api_params = {"series": indicator_code,
                      "db": db,
                      "labels": include_labels,
                      "skipBlanks": skip_blanks,
                        "skipAggs": skip_aggs,
                      "economy": entity_code,
                      "time": time,
                      "numericTimeKeys": True,
                      "params": params,
                      **extra
                      }

        # remove any nulls
        api_params = {k: v for k, v in api_params.items() if v is not None}

        logger.info("Fetching data from World Bank API...")

        try:
            wb_generator = wb.data.fetch(**api_params)
            df = WorldBank._parse_df(wb_generator)

        except Exception as e:
            raise DataExtractionError(f"Failed to fetch data from World Bank API. Error: {e}")

        logger.info("Data fetched successfully from World Bank API.")
        return df

    @staticmethod
    def _get_time_range(start: int | None, end: int | None) -> range | None:
        """Get a range of years from start to end, inclusive."""

        # if both are None, return None
        if start is None and end is None:
            return None

        # set defaults
        if start is None:
            start = 1800 # set a very early year

        if end is None:
            end = 2099 # set a very late year

        return range(start, end + 1)

    @staticmethod
    def _parse_df(wb_generator: Generator) -> pd.DataFrame:
        """Parse the generator returned from wbgapi into a DataFrame."""

        # check if any data was returned
        try:
            first = next(wb_generator)
        except StopIteration:
            raise DataExtractionError("No data returned from World Bank API.")

        data = [first] # initialize with the first row
        for row in wb_generator:
            data.append(row)

        # flatten the structure
        return pd.json_normalize(data, sep="_")


    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the dataframe returned from the World Bank API into standard format.

        Steps
        - rename columns to standard names
        - drop unnecessary columns (time_id)
        - convert dtypes to pyarrow compatible types

        """

        # cleaning steps
        if "time_id" in df.columns:
            df = df.drop(columns=["time_id"])
        if "time_value" in df.columns:
            df = df.rename(columns={"time_value": Fields.year})
        if "time" in df.columns:
            df = df.rename(columns={"time": Fields.year})

        # replace "economy" columns
        if "economy" in df.columns:
            df = df.rename(columns={"economy": Fields.entity_code})
        if "economy_id" in df.columns:
            df = df.rename(columns={"economy_id": Fields.entity_code})
        if "economy_value" in df.columns:
            df = df.rename(columns={"economy_value": Fields.entity_name})

        # replace "aggregate" columns
        if "economy_aggregate" in df.columns:
            df = df.rename(columns={"economy_aggregate": "is_aggregate"})
        if "aggregate" in df.columns:
            df = df.rename(columns={"aggregate": "is_aggregate"})

        # replace series columns
        if "series" in df.columns:
            df = df.rename(columns={"series": Fields.indicator_code})
        if "series_id" in df.columns:
            df = df.rename(columns={"series_id": Fields.indicator_code})
        if "series_value" in df.columns:
            df = df.rename(columns={"series_value": Fields.indicator_name})

        # replace counterpart area columns
        if "counterpart_area" in df.columns:
            df = df.rename(columns={"counterpart_area": Fields.counterpart_code})
        if "counterpart_area_id" in df.columns:
            df = df.rename(columns={"counterpart_area_id": Fields.counterpart_code})
        if "counterpart_area_value" in df.columns:
            df = df.rename(columns={"counterpart_area_value": Fields.counterpart_name})

        # replace value column
        if "value" in df.columns:
            df = df.rename(columns={"value": Fields.value})

        # convert dtypes
        df = convert_dtypes(df)

        return df

    def get_data(self,
                 indicator_code: str | list[str],
                 db: int | None = None,
                 entity_code: str | list[str] | None = None,
                 start_year: int | None = None,
                 end_year: int | None = None,
                 skip_blanks: bool = False,
                 skip_aggs: bool = False,
                 include_labels: bool = False,
                 *,
                 params: dict | None = None,
                 **kwargs
                 ) -> pd.DataFrame:
        """ Get the data as a dataframe

        Args:
            indicator_code: an indicator code or list of indicator codes
            db: the database id. If None, uses the currently set database.
            entity_code: an economy code or list of economy codes. If None, all economies are included.
            start_year: the start year for the data. If None, uses the earliest available year.
            end_year: the end year for the data. If None, uses the latest available year.
            skip_blanks: whether to skip blank observations
            skip_aggs: whether to skip aggregate entities
            include_labels: whether to include labels instead of codes. Defaults to False.
            params: extra query parameters to pass to the API
            **kwargs: extra dimensions, database specific (e.g., version)
                - mrv: return only the specified number of most recent values (same time period for all economies)
                - mrnev: return only the specified number of non-empty most recent values (time period varies)
                **NOTE**: these two parameters are available in the wbgapi but do not always behave as expected with certain databases

        Returns:
            A DataFrame with the requested data.

        """

        if db is None:
            db = self.db
        else:
            logger.info(f"Using database {db}.")

        self._check_valid_db(db)

        # if isinstance(indicator_code, str):
        #     indicator_code = [indicator_code]

        # check that all the indicators exist in the database
        # available_indicators = self.get_available_indicators(db)[Fields.indicator_code].tolist()
        # for ind in indicator_code:
        #     if ind not in available_indicators:
        #         raise ValueError(f"Indicator code {ind} is not available in database {db}.")

        df = self._fetch_data(
            indicator_code=tuple(indicator_code) if isinstance(indicator_code, list) else indicator_code,
            db=db,
            entity_code=tuple(entity_code) if isinstance(entity_code, list) else entity_code,
            time = self._get_time_range(start_year, end_year),
            skip_blanks=skip_blanks,
            skip_aggs=skip_aggs,
            include_labels=include_labels,
            params_items=_make_hashable(params),
            extra_items=_make_hashable(kwargs)

        )

        df = self._clean_df(df)

        # validate dataframe
        DataFrameValidator().validate(df, required_cols=[Fields.year, Fields.indicator_code, Fields.entity_code, Fields.value])

        return df

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._fetch_data.cache_clear()
        # TODO: Clear class attributes
        self._indicators = {}
        self._databases = None

        logger.info("Cache cleared.")






