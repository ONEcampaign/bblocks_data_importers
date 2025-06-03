import json
from functools import wraps
from typing import Optional, Literal, Iterable

import pandas as pd

from bblocks.data_importers.config import Paths, Fields
from bblocks.data_importers.world_bank.wb_api import WorldBank


def read_indicators_file(file: str) -> dict:
    """Read a json which contains the IDS indicators stored in the requested file

    Args:
        file (str): The file to read - either 'ids','debt_stocks', or 'debt_service'.
         Defaults to "ids"
    """
    with open(Paths.wb_importer / f"{file}_indicators.json", "r") as fp:
        return json.load(fp)


def get_indicator_groupings(name: str, detailed_category: bool = False):
    """Fetch indicators from json files stored inside the module.

    Args:
        name: the name of the indicators to fetch.
        detailed_category: whether to return detailed categories or not.

    Returns:
        A dictionary of debt service indicators.
    """
    return {
        k: v["detailed_category" if detailed_category else "broad_category"]
        for k, v in read_indicators_file(name).items()
    }


def _group_by_indicator(data: pd.DataFrame, remove_from_index: list[str] = None):
    """Group data by indicator and sum values.

    Args:
        data: the data to group.
        remove_from_index: columns to remove from the index (besides 'value').

    Returns:
        A DataFrame with the data grouped.

    """
    idx = [c for c in data.columns if c not in remove_from_index + ["value"]]

    return data.groupby(idx, observed=True, dropna=False)[["value"]].sum().reset_index()


class InternationalDebtStatistics(WorldBank):
    """World Bank Data Importer.

    This class provides a simplified interface for fetching and managing data from the
    International Debt Statistics database.

    It leverages the `wbgapi` package to interact with the World Bank API, making it easy to retrieve
    and clean development indicators for analysis.

    The class supports configurable options for economies, years, and databases, and allows fine-tuning
    of API parameters. Users can set configurations such as which economies or years to fetch data for,
    whether to retrieve the most recent data, and more.
    """

    def __init__(self):
        super().__init__()
        self.set_database(6)
        self.config["api_params"]["per_page"] = 50_000_000

    def __repr__(self) -> str:
        """String representation of the InternationalDebtStatistics object."""

        if self._data is not None:
            loaded = sorted(self._data[Fields.indicator_code].unique().tolist())
        else:
            loaded = []

        return (
            f"{self.__class__.__name__}("
            f"database={self.config['database']!r}, "
            f"latest_update={self.latest_update!r}, "
            f"economies={self.config['economies']!r}, "
            f"years={self.config['years']!r}, "
            f"loaded_series={loaded!r}, "
            f")"
        )

    @property
    def latest_update(self):
        return self.api.source.info(id=self.config["database"]).items[0]["lastupdated"]

    @property
    def available_indicators(self):
        return self.api.series.Series().to_dict()

    @staticmethod
    def debt_service_indicators(
        detailed_category: bool = False,
    ):
        """Fetch debt service indicators.

        Args:
            detailed_category: whether to return detailed categories or not.

        Returns:
            A dictionary of debt service indicators.
        """
        return get_indicator_groupings("debt_service", detailed_category)

    @staticmethod
    def debt_stocks_indicators(detailed_category: bool = False):
        """Fetch debt stocks indicators.

        Args:
            detailed_category: whether to return detailed categories or not.

        Returns:
            A dictionary of debt stocks indicators.
        """
        return get_indicator_groupings("debt_stocks", detailed_category)

    def counterpart_names_to_entity_codes(self) -> dict:
        codes_to_names = self.api.Series(
            self.api.source.features("counterpart_area")
        ).to_dict()

        names_to_iso3 = self.api.economy.coder(name=codes_to_names.values())

        additional_mapping = {
            "WLD": "WLD",
            "063": "YUG",
            "073": "CSK",
            "074": "DDR",
            "264": "REU",
            "345": "GLP",
            "361": "ANT",
            "376": "AIA",
            "377": "ATG",
            "379": "VIR",
            "457": "SUR",
            "740": "PRK",
        }

        return {
            k: names_to_iso3.get(v) for k, v in codes_to_names.items()
        } | additional_mapping

    def get_debt_service_data(
        self,
        detailed_category: bool = False,
        years: Optional[Literal["all"] | int | list[int] | Iterable] = None,
        economies: Optional[str | list[str] | Literal["all"]] = None,
    ):
        """Get debt service data.

        Args:
            detailed_category: whether to return detailed categories or not.
            years: Optional[Literal["all"] | int | list[int] | Iterable]: Optionally, set the years to
             fetch data for. Specifying years here will override the instance configuration.
             All years are fetched by default
            economies (Optional[str | list[str] | Literal["all"]]): Optionally, set the economies to
                fetch data for. Specifying economies here will override the instance configuration.
                All economies are fetched by default.

        Returns:
            A DataFrame with debt service data.
        """

        indicators = self.debt_service_indicators(detailed_category)

        data = (
            self.get_data(list(indicators), years=years, economies=economies)
            .assign(indicator=lambda d: d["indicator_code"].map(indicators))
            .pipe(_group_by_indicator, remove_from_index=["indicator_code"])
        )

        return data

    def get_debt_stocks_data(
        self,
        detailed_category: bool = False,
        years: Optional[Literal["all"] | int | list[int] | Iterable] = None,
        economies: Optional[str | list[str] | Literal["all"]] = None,
    ):
        """Get debt stocks data.

        Args:
            detailed_category: whether to return detailed categories or not.
            years: Optional[Literal["all"] | int | list[int] | Iterable]: Optionally, set the years to
             fetch data for. Specifying years here will override the instance configuration.
             All years are fetched by default
            economies (Optional[str | list[str] | Literal["all"]]): Optionally, set the economies to
                fetch data for. Specifying economies here will override the instance configuration.
                All economies are fetched by default.

        Returns:
            A DataFrame with debt stocks data.
        """

        indicators = self.debt_stocks_indicators(detailed_category)

        data = (
            self.get_data(list(indicators), years=years, economies=economies)
            .assign(indicator=lambda d: d["indicator_code"].map(indicators))
            .pipe(_group_by_indicator, remove_from_index=["indicator_code"])
        )

        return data

    def add_counterpart_iso3_codes(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add counterpart ISO3 codes to the data.

        Args:
            data: The data to add counterpart ISO3 codes to.

        Returns:
            The data with counterpart ISO3 codes added.
        """
        # Insert counterpart_entity_code column right after counterpart_code
        mapping = self.counterpart_names_to_entity_codes()

        # Find the index of counterpart_code
        idx = data.columns.get_loc("counterpart_code")

        # Insert counterpart_name column
        data.insert(
            idx + 1, "counterpart_entity_code", data["counterpart_code"].map(mapping)
        )

        return data

    def add_counterpart_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add counterpart names to the data.

        Args:
            data: The data to add counterpart names to.

        Returns:
            The data with counterpart names added.
        """
        # Insert counterpart_entity_code column right after counterpart_code
        mapping = self.api.Series(
            self.api.source.features("counterpart_area")
        ).to_dict()

        # Find the index of counterpart_code
        idx = data.columns.get_loc("counterpart_code")

        # Insert counterpart_name column
        data.insert(idx + 1, "counterpart_name", data["counterpart_code"].map(mapping))

        return data

    @wraps(WorldBank.get_data)
    def get_data(
        self,
        series: str | list[str],
        years: Optional[Literal["all"] | int | list[int] | Iterable] = None,
        economies: Optional[str | list[str] | Literal["all"]] = None,
        config: Optional[dict] = None,
    ):
        # Get data
        data = (
            super()
            .get_data(series=series, years=years, economies=economies, config=config)
            .pipe(self.add_counterpart_names)
            .pipe(self.add_counterpart_iso3_codes)
        )

        return data
