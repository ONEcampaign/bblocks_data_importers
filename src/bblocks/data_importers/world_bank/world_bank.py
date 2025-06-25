"""World Bank"""

import pandas as pd
import wbgapi as wb

from bblocks.data_importers.config import logger


class WorldBank:
    """ """

    def __init__(self):

        self._data: dict = {}
        self._databases: pd.DataFrame | None = None
        self._db: int | None = None

    def get_available_databases(self):
        """ """

        if self._databases is None:
            self._databases = pd.DataFrame(wb.source.info().table()).set_axis(wb.source.info().columns, axis=1)

        return self._databases

    @property
    def db(self):
        if self._db is None:
            raise AttributeError("The database has not been set yet. Use the `set_db` method to set the database"
                                 " or use the `get_available_databases` method to see the available databases."
                                 " By default, calling the `get_data` method without setting a database will query the"
                                 " World Development Indicators database (id=2)")
        return self._db

    def get_data(self, series: str | list[str], db: int | None = None):
        """ """

