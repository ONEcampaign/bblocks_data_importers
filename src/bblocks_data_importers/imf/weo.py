"""Importer for the WEO database from IMF"""

from typing import Literal

import pandas as pd
from imf_reader import weo

from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes
from bblocks_data_importers.config import logger, weo_version, Fields


class WEO(DataImporter):
    """Importer for the WEO database from IMF"""

    def __init__(self):
        self._data: dict = {}
        self._latest_version = None

    @staticmethod
    def _format_data(df: pd.DataFrame):
        """Format WEO data"""

        return (df
                .pipe(convert_dtypes)
                .rename(columns={"OBS_VALUE": Fields.value,
                                 "TIME_PERIOD": Fields.year,
                                 "REF_AREA_CODE": Fields.entity_code,
                                 "REF_AREA_LABEL": Fields.entity_name,
                                 "CONCEPT_CODE": Fields.indicator_code,
                                 "CONCEPT_LABEL": Fields.indicator_name,
                                 "UNIT_LABEL": Fields.unit,
                                 "LASTACTUALDATE": "estimates_start_year",
                                 })
                # convert other columns to lowercase
                .rename(columns={col: col.lower() for col in df.columns})
                )

    def _load_data(self, version=None) -> None:
        """Load WEO data to the object for a specific version

        Args:
            version: version of the WEO data to load. If None, the latest version is loaded
        """

        self._data[weo.fetch_data.last_version_fetched] = weo.fetch_data(version).pipe(self._format_data)

        # if the latest version is loaded, save the version to _latest_version
        if version is None:
            self._latest_version = weo.fetch_data.last_version_fetched

    def get_data(self, version: weo_version = "latest") -> pd.DataFrame:
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
