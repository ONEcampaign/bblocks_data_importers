"""Importer for the World Bank

The World Bank offers data through its API for various databases, such as the World Development Indicators (WDI).

This importer provides functionality to get data for different World Bank databases, access metadata on indicators
and entities, and see the available databases.

Usage example:

To see the available databases:
>>> print(get_wb_databases())
This will return a DataFrame with the available World Bank databases, their IDs, names, codes, and last updated dates.
You will need the IDs to specify which database to query.

To start querying data, fisrt create an instance of the WorldBank class, specifying the database ID you want to use:
>>> wb_importer = WorldBank(db=2)  # 2 is the ID for

By default, if no database is specified, the World Development Indicators database (id=2) is used.
>>> wb_importer = WorldBank()  # uses WDI by default


To get available indicators in the database:
>>> indicators_df = wb_importer.get_available_indicators()
This will return a DataFrame with the available indicator (their codes and names) for the specified database. You
will need the indicator codes to query data.

To get data for specific indicators:
>>> data_df = wb_importer.get_data(indicator_code='SP.POP.TOTL') # total population indicator code
This will return a DataFrame with the data for the specified indicator across all entities and years
available in the database.

Multiple indicators can be queried. Batching and multithreading are used to speed up data retrieval for
multiple indicators. Indicators are batched into groups of 1 by default, and 4 threads are used for fetching data.
Different batch sizes and thread numbers can be specified using the `batch_size` and `thread_num` parameters.

Other parameters can be used to filter the data, such as specifying entity codes, year ranges, whether to skip blank
observations, whether to include labels, etc. and any other parameter supported by the World Bank API (read the wbgapi
documentation for more details)[https://github.com/tgherzog/wbgapi]

>>> data_df = wb_importer.get_data(
...     indicator_code=['SP.POP.TOTL', 'NY.GDP.MKTP.CD'], # total population and gdp indicator codes
...     entity_code=['ZWE', 'KEN'], # Zimbabwe and Kenya
...     start_year=2000,
...     end_year=2020,
...     skip_blanks=True,
...     include_labels=False,
...     batch_size=2,
...     thread_num=2)

Data is cached by default to avoid redundant API calls for the same queries. To clear the cache, use the `clear_cache` method:
>>> wb_importer.clear_cache()

To get metadata for specific indicators:
>>> metadata_df = wb_importer.get_indicator_metadata(indicator_code='SP.POP.TOTL')
This will return a DataFrame with the metadata for the specified indicator.

To specify entities to query, their codes need to be used. To get the available entities and their codes, as well
as other entity metadata such as region and income level, use the `get_available_entities` method:
>>> entities_df = wb_importer.get_available_entities()
This will return a DataFrame with the available entities and their metadata for the specified database.

To get all the available entities maintained by the World Bank across all databases, use the `get_wb_entities` function:
>>> all_entities_df = get_wb_entities()
This will return a DataFrame with all the available entities and their metadata.

Optionally aggregate entities can be skipped using the `skip_aggs` parameter:
>>> entities_df = wb_importer.get_available_entities(skip_aggs=True)
This will return a DataFrame with the available non-aggregate entities and their metadata for the specified
"""

from functools import lru_cache
from collections.abc import Hashable
from typing import Generator
import pandas as pd
import wbgapi as wb
from concurrent.futures import ThreadPoolExecutor, as_completed


from bblocks.data_importers.config import logger, Fields, DataExtractionError
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.data_validators import DataFrameValidator


_BATCH_SIZE: int = 1  # number of indicators to fetch per batch
_NUM_THREADS: int = 4  # number of threads to use for fetching data
_PER_PAGE: int = 50_000_000  # number of records per page to request from World Bank API


def _batch(iterable: list[str], n: int) -> Generator:
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


@lru_cache
def get_wb_databases() -> pd.DataFrame:
    """Get the available World Bank databases.

    Returns:
        A DataFrame with the available World Bank databases.
    """

    l = [i for i in wb.source.list()]
    df = (
        pd.json_normalize(l)
        .rename(columns={"lastupdated": "last_updated"})
        .loc[:, ["id", "name", "code", "last_updated"]]
    )

    # convert id to integer
    df["id"] = df["id"].astype(int)

    # convert last_updated to datetime
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    return convert_dtypes(df)


@lru_cache(maxsize=None)
def get_wb_entities(db: int | None = None, skip_aggs: bool = False) -> pd.DataFrame:
    """Get entities available in World Bank databases or a specific database, including metadata.

    Args:
        db: The database id. If None, uses the global database.
        skip_aggs: Whether to skip aggregate entities.

    Returns:
        A DataFrame with the available entities for the specified database including their metadata.
    """

    l = [i for i in wb.economy.list(db=db, labels=True, skipAggs=skip_aggs)]
    df = pd.json_normalize(l, sep="_")

    cols = {
        "id": Fields.entity_code,
        "value": Fields.entity_name,
        "aggregate": "is_aggregate",
        "longitude": "longitude",
        "latitude": "latitude",
        "capitalCity": "capital_city",
        "region_id": Fields.region_code,
        "region_value": Fields.region_name,
        "adminregion_id": "admin_region_code",
        "adminregion_value": "admin_region_name",
        "lendingType_id": "lending_type_code",
        "lendingType_value": "lending_type_name",
        "incomeLevel_id": Fields.income_level_code,
        "incomeLevel_value": Fields.income_level_name,
    }

    df = df.rename(columns=cols).loc[:, cols.values()]

    # find any empty strings and replace with NaN
    df = df.replace("", pd.NA)

    return convert_dtypes(df)


@lru_cache(maxsize=None)
def get_wb_indicators(db: int | None = None) -> pd.DataFrame:
    """Get indicators available in World Bank databases or a specific database

    Args:
        db: The database id. If None, uses the global database.

    Returns:
        A DataFrame with the available indicators for the specified database.
    """

    l = [i for i in wb.series.list(db=db)]
    df = pd.json_normalize(l).rename(
        columns={"id": Fields.indicator_code, "value": Fields.indicator_name}
    )

    return convert_dtypes(df)


def _check_valid_db(db: int) -> None:
    """Check if the provided database id is valid."""

    if db not in get_wb_databases()["id"].unique():
        raise ValueError(f"Database ID {db} is not valid.")


@lru_cache(maxsize=None)
def _get_cached_metadata(**kwargs) -> list:
    """Helper function to get cached metadata from World Bank API."""

    return [i for i in wb.series.metadata.fetch(**kwargs)]


def get_indicator_metadata(
    indicator_code: str | list[str], db: int | None = None
) -> pd.DataFrame:
    """Get indicator metadata for a given indicator code.

    Args:
        indicator_code: The indicator code.
        db: The database id. If None, uses the global database.

    Returns:
        A DataFrame with the indicator metadata.
    """

    if isinstance(indicator_code, str):
        indicator_code = [indicator_code]

    # remove duplicates and sort
    indicator_code = sorted(set(indicator_code))

    metadata = _get_cached_metadata(db=db, id=tuple(indicator_code))

    # check if no metadata was returned
    if not metadata:
        raise DataExtractionError(f"No metadata found for indicator code(s).")

    # check that all requested indicators have metadata - if lengths don't match, some indicators are missing
    if len(metadata) != len(indicator_code):
        raise DataExtractionError(f"Metadata not found for some indicator code(s).")

    return pd.DataFrame(
        [
            {Fields.indicator_code: indicator_code[i], **metadata[i].metadata}
            for i in range(len(indicator_code))
        ]
    ).rename(
        columns={
            "IndicatorName": Fields.indicator_name,
            "Aggregationmethod": "aggregation_method",
            "Dataset": "dataset",
            "Developmentrelevance": "development_relevance",
            "License_Type": "license_Type",
            "License_URL": "license_url",
            "Limitationsandexceptions": "limitations_and_exceptions",
            "Longdefinition": "long_definition",
            "Othernotes": "other_notes",
            "Periodicity": "periodicity",
            "Referenceperiod": "reference_period",
            "Shortdefinition": "short_definition",
            "Source": "source",
            "Statisticalconceptandmethodology": "statistical_concept_and_methodology",
            "Topic": "topic",
            "Unitofmeasure": Fields.unit,
        }
    )


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
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


def get_data(api_params: dict) -> pd.DataFrame:
    """Fetch data from the World Bank API using wbgapi with the provided parameters. And parse the
    response into a cleaned dataframe.

    If no data is returned from the api, an empty dataframe is returned.
    """

    try:
        l = [i for i in wb.data.fetch(**api_params)]

        # if no data was returned, return empty dataframe
        if not l:
            return pd.DataFrame()

        df = pd.json_normalize(l, sep="_")
        return _clean_df(df)

    except Exception as e:
        raise DataExtractionError(
            f"Failed to fetch data from World Bank API. Error: {e}"
        )


def _get_time_range(start: int | None, end: int | None) -> range | None:
    """Get a range of years from start to end, inclusive."""

    # if both are None, return None
    if start is None and end is None:
        return None

    # set defaults
    if start is None:
        start = 1800  # set a very early year

    if end is None:
        end = 2099  # set a very late year

    return range(start, end + 1)


def _make_cache_key(
        *,
        indicators: list[str],
        db: int | None,
        entity_code: list[str] | None,
        time: range | None,
        skip_blanks: bool,
        skip_aggs: bool,
        include_labels: bool,
        params: dict | None,
        extra: dict,
) -> tuple:
    """Build a hashable, canonical cache key for a data request."""
    return (
        db,
        tuple(indicators),
        tuple(entity_code) if entity_code is not None else None,
        None if time is None else (time.start, time.stop),
        skip_blanks,
        skip_aggs,
        include_labels,
        frozenset(params.items()) if params else None,
        frozenset(extra.items()) if extra else None,
    )


class WorldBank:
    """Importer for World Bank data.

    The World Bank offers data through its API for various databases, such as the World Development Indicators (WDI).
    This importer provides functionality to connect to a specified World Bank database, get data for different indicators,
    and access metadata on indicators and entities.

    Usage example:

    This class connects to a specified World Bank database using its ID. To see the available databases,
    use the `get_wb_databases` function.
    >>> print(get_wb_databases())

    Instantiate the WorldBank class, specifying the database ID you want to use:
    >>> wb_importer = WorldBank(db=2)  # 2 is the ID

    By default, if no database is specified, the World Development Indicators database (id=2) is used.
    >>> wb_importer = WorldBank()  # uses WDI by default

    To get available indicators in the database:
    >>> indicators_df = wb_importer.get_available_indicators()

    To get data for specific indicators:
    >>> data_df = wb_importer.get_data(indicator_code='SP.POP.TOTL') # total population indicator code

    Multiple indicators can be queried. Batching and multithreading are used to speed up data retrieval for
    multiple indicators. Indicators are batched into groups of 1 by default, and 4 threads are used for fetching data.
    Different batch sizes and thread numbers can be specified using the `batch_size` and `thread_num` parameters.
    Additional parameters can be used including specifying entity codes, year ranges, whether to skip blank
    observations, whether to include labels, etc. and any other parameter supported by the World Bank API (read the wbgapi
    documentation for more details)[https://github.com/tgherzog/wbgapi]

    >>> data_df = wb_importer.get_data(
    ...     indicator_code=['SP.POP.TOTL', 'NY.GDP.MKTP.CD'], # total population and gdp indicator codes
    ...     entity_code=['ZWE', 'KEN'], # Zimbabwe and Kenya
    ...     start_year=2000,
    ...     end_year=2020,
    ...     skip_blanks=True,
    ...     include_labels=False,
    ...     batch_size=2,
    ...     thread_num=2)

    To get metadata for specific indicators:
    >>> metadata_df = wb_importer.get_indicator_metadata(indicator_code='SP.POP.TOTL')

    To get the available entities and their codes, as well as other entity metadata such as region and income level,
    use the `get_available_entities` method:
    >>> entities_df = wb_importer.get_available_entities()

    Data is cached by default to avoid redundant API calls for the same queries. To clear the cache, use the `clear_cache` method:
    >>> wb_importer.clear_cache()

    """

    def __init__(self, db: int | None = None):

        # Set the database if provided
        if db is not None:
            _check_valid_db(db)
            self._db = db
        else:
            self._db = wb.db

        self._data_cache: dict[Hashable, pd.DataFrame] = {}

        logger.info(f"World Bank database set to {self._db}.")

    @property
    def db(self) -> int:
        """Get the current World Bank database."""

        if self._db is None:
            raise AttributeError(
                "The database has not been set yet. Use the `set_db` method to set the database"
                " or use the `get_available_databases` method to see the available databases."
                " By default, calling the `get_data` method without setting a database will query the"
                " World Development Indicators database (id=2)"
            )
        return self._db

    def get_available_indicators(self) -> pd.DataFrame:
        """Get available indicators in the database

        Returns:
            A DataFrame with the available indicators for the specified database.
        """

        return get_wb_indicators(db=self.db)

    def get_available_entities(self, skip_aggs: bool = False) -> pd.DataFrame:
        """Get available economies for a database

        Args:
            skip_aggs: Whether to skip aggregate entities.

        Returns:
            A DataFrame with the available entities and their metadata
        """

        return get_wb_entities(db=self.db, skip_aggs=skip_aggs)

    def get_indicator_metadata(self, indicator_code: str | list[str]) -> pd.DataFrame:
        """Get indicator metadata for a given indicator code.

        Args:
            indicator_code: The indicator code.

        Returns:
            A dictionary with the indicator metadata.
        """

        return get_indicator_metadata(indicator_code=indicator_code, db=self.db)

    def _fetch_data(
            self,
            *,
            indicators: list[str],
            db: int | None,
            entity_code: list[str],
            time: range | None,
            skip_blanks: bool,
            skip_aggs: bool,
            include_labels: bool,
            params: dict | None,
            extra: dict,
            batch_size: int,
            thread_num: int,
    ) -> pd.DataFrame:
        """Fetch data from the World Bank API.

        This method handles preparing the wbgapi parameters, fetching the data by batching indicators and
        multithreading for faster retrieval. Data is cached using last recently used (LRU) caching to avoid
        redundant API calls for the same queries. Cache size is limited to 8 unique queries (due to the potential
        size of World Bank API responses).
        """

        # get the hash key
        # check if hash key exists in cache
        # if exists, return cached dataframe
        # else fetch the data and store in cache and return the dataframe

        cache_key = _make_cache_key(
            indicators=indicators,
            db=db,
            entity_code=entity_code,
            time=time,
            skip_blanks=skip_blanks,
            skip_aggs=skip_aggs,
            include_labels=include_labels,
            params=params,
            extra=extra,
        )

        # if key exists in cache return cached dataframe
        cached = self._data_cache.get(cache_key)
        if cached is not None:
            return cached


        logger.info("Fetching data from World Bank API...")

        api_params = {
            "db": db,
            "labels": include_labels,
            "skipBlanks": skip_blanks,
            "skipAggs": skip_aggs,
            "economy": entity_code,
            "time": time,
            "numericTimeKeys": True,
            "params": params,
            **extra,
        }
        # remove None values
        api_params = {k: v for k, v in api_params.items() if v is not None}

        # fetch data in batches using multithreading
        batches = _batch(indicators, batch_size)  # create batches of indicators
        results = []  # results list

        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            futures = []
            for batch_indicators in batches:
                futures.append(
                    executor.submit(
                        get_data, {**api_params, "series": batch_indicators}
                    )
                )

            for future in as_completed(futures):
                results.append(future.result())

        # concatenate results
        df = pd.concat(results, ignore_index=True)

        # if the dataframe is empty raise an error
        if df.empty:
            raise DataExtractionError("No data returned from World Bank API.")

        # validate dataframe
        DataFrameValidator().validate(
            df,
            required_cols=[
                Fields.year,
                Fields.indicator_code,
                Fields.entity_code,
                Fields.value,
            ],
        )

        logger.info("Data fetched successfully from World Bank API.")

        # store in cache
        self._data_cache[cache_key] = df
        return df

    def get_data(
        self,
        indicator_code: str | list[str],
        entity_code: str | list[str] | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
        skip_blanks: bool = False,
        skip_aggs: bool = False,
        include_labels: bool = False,
        *,
        params: dict | None = None,
        batch_size: int = _BATCH_SIZE,
        thread_num: int = _NUM_THREADS,
        **kwargs,
    ) -> pd.DataFrame:
        """Get World Bank data for specified indicators

        This function queries the World Bank database API for specified indicators and other parameters,
        returning the data as a pandas DataFrame.

        Args:
            indicator_code: an indicator code or list of indicator codes
            entity_code: an economy code or list of economy codes. If None, all economies are included.
            start_year: the start year for the data. If None, uses the earliest available year.
            end_year: the end year for the data. If None, uses the latest available year.
            skip_blanks: whether to skip blank observations
            skip_aggs: whether to skip aggregate entities
            include_labels: whether to include labels instead of codes. Defaults to False.
            params: extra query parameters to pass to the API
                per_page sets the number of records to return per page. Defaults to 50,000,000.
            batch_size: number of indicators to fetch per batch. Defaults to 1.
            thread_num: number of threads to use for fetching data. Defaults to 4.
            **kwargs: extra dimensions, database specific (e.g., version)
                - mrv: return only the specified number of most recent values (same time period for all economies)
                - mrnev: return only the specified number of non-empty most recent values (time period varies)
                **NOTE**: these two parameters are available in the wbgapi but do not always behave as expected with certain databases

        Returns:
            A DataFrame with the requested data.

        """

        # normalise indicators
        if isinstance(indicator_code, str):
            indicator_code = [indicator_code]
        indicators = sorted(set(indicator_code))

        # normalise entity codes
        if entity_code is not None:
            if isinstance(entity_code, str):
                entity_code = [entity_code]
            entities = sorted(set(entity_code))
        else:
            entities = None

        # ensure params exists and set per_page
        params = {} if params is None else dict(params)
        params.setdefault("per_page", _PER_PAGE)

        time_range = _get_time_range(start_year, end_year)

        df = self._fetch_data(
            indicators=indicators,
            db=self._db,
            entity_code=entities,
            time=time_range,
            skip_blanks=skip_blanks,
            skip_aggs=skip_aggs,
            include_labels=include_labels,
            params=params,
            extra=kwargs,
            batch_size=batch_size,
            thread_num=thread_num,
        )

        # shallow copy to avoid accidental mutation of cached df
        return df.copy(deep=False)

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._data_cache = {} # reset the cache dictionary

        logger.info("Cache cleared.")
