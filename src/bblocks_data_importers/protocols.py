"""
Protocol for data importers.
"""

from typing import Protocol
import pandas as pd


class DataImporter(Protocol):
    """Protocol for data importers.

    Data importers are classes that are responsible for importing data from
    external sources and providing it to user in a structured format.

    Methods:
    - get_data: get the data from the source.
    """

    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        """Method to return data as a pandas DataFrame."""
        ...
