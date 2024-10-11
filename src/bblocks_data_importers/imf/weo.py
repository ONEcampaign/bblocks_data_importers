"""Importer for the WEO database from IMF"""

from typing import Literal, Tuple

from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes

class WEO(DataImporter):
    """Importer for the WEO database from IMF"""

    def __init__(self):

        self._data: None | list[dict] = None

    def get_data(self, version: Tuple[Literal["April", "October"], int]):  # Type hint for valid months
        pass

    def clear_cache(self):
        pass



