"""Importer for the WEO database from IMF"""

from typing import Literal

import pandas as pd
from imf_reader import weo

from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes
from bblocks_data_importers.config import logger


class WEO(DataImporter):
    """Importer for the WEO database from IMF"""

    def __init__(self):
        self._data: dict = {}
        self._latest_version = None

    def _load_data(self, version = None) -> None:
        """Load WEO data to the object for a specific version

        Args:
            version: version of the WEO data to load. If None, the latest version is loaded
        """

        self._data[weo.fetch_data.last_version_fetched] = weo.fetch_data(version)

        if version is None:
            self._latest_version = weo.fetch_data.last_version_fetched

    def get_data(self, version: Literal["latest"] | tuple[Literal['April', 'October'], int] = "latest") -> pd.DataFrame:
        """Get the WEO data for a specific version

        Args:
            version: version of the WEO data to get. If "latest", the latest version is returned.
                    If another version is required, pass a tuple with the month and year of the version.
                    WEO releases data in April and October each year.

        Returns:
            The WEO data for the specified version
        """

        if version == "latest":
            if self._latest_version is not None:
                return self._data[self._latest_version]
            else:
                self._load_data()
                return self._data[self._latest_version]

        if version not in self._data:
            self._load_data(version)
            return self._data[version]

    def clear_cache(self):
        """Clear the data cached in the importer"""

        self._latest_version = None
        logger.info("Cache cleared")
